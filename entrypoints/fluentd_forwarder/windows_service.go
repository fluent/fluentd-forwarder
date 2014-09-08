// +build windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	ERROR_FAILED_SERVICE_CONTROLLER_CONNECT = 1063
)

const (
	SERVICE_FILE_SYSTEM_DRIVER           = 0x00000002
	SERVICE_KERNEL_DRIVER                = 0x00000001
	SERVICE_WIN32_OWN_PROCESS            = 0x00000010
	SERVICE_WIN32_SHARE_PROCESS          = 0x00000020
	SERVICE_INTERACTIVE_PROCESS          = 0x00000100
	SERVICE_CONTINUE_PENDING             = 0x00000005
	SERVICE_PAUSE_PENDING                = 0x00000006
	SERVICE_PAUSED                       = 0x00000007
	SERVICE_RUNNING                      = 0x00000004
	SERVICE_START_PENDING                = 0x00000002
	SERVICE_STOP_PENDING                 = 0x00000003
	SERVICE_STOPPED                      = 0x00000001
	SERVICE_ACCEPT_NETBINDCHANGE         = 0x00000010
	SERVICE_ACCEPT_PARAMCHANGE           = 0x00000008
	SERVICE_ACCEPT_PAUSE_CONTINUE        = 0x00000002
	SERVICE_ACCEPT_PRESHUTDOWN           = 0x00000100
	SERVICE_ACCEPT_SHUTDOWN              = 0x00000004
	SERVICE_ACCEPT_STOP                  = 0x00000001
	SERVICE_ACCEPT_HARDWAREPROFILECHANGE = 0x00000020
	SERVICE_ACCEPT_POWEREVENT            = 0x00000040
	SERVICE_ACCEPT_SESSIONCHANGE         = 0x00000080
	SERVICE_ACCEPT_TIMECHANGE            = 0x00000200
	SERVICE_ACCEPT_TRIGGEREVENT          = 0x00000400
	SERVICE_ACCEPT_USERMODEREBOOT        = 0x00000800
)

var (
	modadvapi32                      = syscall.NewLazyDLL("advapi32.dll")
	procStartServiceCtrlDispatcherW  = modadvapi32.NewProc("StartServiceCtrlDispatcherW")
	procRegisterServiceCtrlHandlerEx = modadvapi32.NewProc("RegisterServiceCtrlHandlerExW")
	procSetServiceStatus             = modadvapi32.NewProc("SetServiceStatus")
)

type ServiceTableEntry struct {
	ServiceName         uintptr
	ServiceMainFunction uintptr
}

type ServiceStatus struct {
	ServiceType             int32
	CurrentState            int32
	ControlsAccepted        int32
	Win32ExitCode           int32
	ServiceSpecificExitCode int32
	CheckPoint              int32
	WaitHint                int32
}

func startServiceCtrlDispatcher(entries *[4096]ServiceTableEntry) (err error) {
	r1, _, e1 := syscall.Syscall(procStartServiceCtrlDispatcherW.Addr(), 1, uintptr(unsafe.Pointer(entries)), 0, 0)
	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func setServiceStatus(handle syscall.Handle, serviceStatus *ServiceStatus) (err error) {
	r1, _, e1 := syscall.Syscall(procSetServiceStatus.Addr(), 2, uintptr(handle), uintptr(unsafe.Pointer(serviceStatus)), 0)
	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func serviceDispatch(name string, handler func([]string, func())) error {
	p_, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return err
	}
	err = startServiceCtrlDispatcher(&[4096]ServiceTableEntry{
		{
			ServiceName: uintptr(unsafe.Pointer(p_)),
			ServiceMainFunction: syscall.NewCallback(func(nargs int32, argv uintptr) uintptr {
				args := make([]string, nargs)
				// http://blogs.msdn.com/b/oldnewthing/archive/2003/12/10/56028.aspx
				_nargs := int(nargs)
				_argv := (*[32768]*[32768]uint16)(unsafe.Pointer(&argv))
				for i := 0; i < _nargs; i += 1 {
					args[i] = syscall.UTF16ToString((*_argv[i])[:])
				}
				handle_, _, _ := syscall.Syscall(procRegisterServiceCtrlHandlerEx.Addr(), 3, uintptr(unsafe.Pointer(p_)), syscall.NewCallback(func() {}), 0)
				handle := syscall.Handle(handle_)
				if handle == syscall.Handle(0) {
					return 0
				}
				setServiceStatus(handle, &ServiceStatus{
					ServiceType:      SERVICE_WIN32_OWN_PROCESS,
					ControlsAccepted: SERVICE_ACCEPT_STOP,
					Win32ExitCode:    0,
					CurrentState:     SERVICE_START_PENDING,
					CheckPoint:       0,
					WaitHint:         10000, // 10sec
				})
				defer setServiceStatus(handle, &ServiceStatus{
					ServiceType:      SERVICE_WIN32_OWN_PROCESS,
					ControlsAccepted: SERVICE_ACCEPT_STOP,
					Win32ExitCode:    0,
					CurrentState:     SERVICE_STOPPED,
					CheckPoint:       0,
					WaitHint:         10000, // 10sec
				})
				handler(args, func() {
					setServiceStatus(handle, &ServiceStatus{
						ServiceType:      SERVICE_WIN32_OWN_PROCESS,
						ControlsAccepted: SERVICE_ACCEPT_STOP,
						Win32ExitCode:    0,
						CurrentState:     SERVICE_RUNNING,
						CheckPoint:       0,
						WaitHint:         10000, // 10sec
					})
				})
				return 0
			}),
		},
		{uintptr(0), uintptr(0)},
	})
	if err == syscall.Errno(ERROR_FAILED_SERVICE_CONTROLLER_CONNECT) {
		handler(os.Args, func() {})
		return nil
	} else {
		return err
	}
}
