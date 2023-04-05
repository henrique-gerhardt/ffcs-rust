# ffcs-rust
Fast File Content Search - Rust

A system that indexes a folder for specific files and index it for search via http requests, also there ia watcher that monitor the file updates and reindex them.

In case of `MaxFilesWatch` error ocourring on linux, increase the inotify limit:

Find the current inotify watch limit by examining the proc file system. In Terminal, run the following:
`cat /proc/sys/fs/inotify/max_user_watches`

Edit /etc/sysctl.conf as root:
`vim /etc/sysctl.conf`

Set (or add if it's not present) the fs.inotify.max_user_watches parameter. Set this to the desired number of watches:
`fs.inotify.max_user_watches=1048576`

Either reboot the system or execute the following command:
`sudo sysctl -p /etc/sysctl.conf`
