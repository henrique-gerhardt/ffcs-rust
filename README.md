# ffcs-rust
Fast File Content Search - Rust

A system that indexes a folder for specific files and index it for search via http requests, also there ia watcher that monitor the file updates and reindex them.

Basic usage:

```cmd
./ffcs -e txt -f /myFolder -i /locationOfIndex
```
Options:
-p, --port <PORT>                  Port where to start the Service [default: 4000]
-e, --file_extension <EXTENSION>   Extension os the files to be search (can be ommited in case of all file extensions)
-f, --root_path <PATH>             Root indexed folder [default: .]  
-i, --index_folder <PATH_INDEX>    Root path where the indexes will be stored [default: ./fileIndex]
-r, --reload_index...              Force re-index of rootpath
-h, --help                         Print help
-V, --version                      Print version

  
Linux caveat:

In case of `MaxFilesWatch` error ocourring on linux, increase the inotify limit:

Find the current inotify watch limit by examining the proc file system. In Terminal, run the following:
```cmd
cat /proc/sys/fs/inotify/max_user_watches
```
Edit /etc/sysctl.conf as root:
```cmd
vim /etc/sysctl.conf
```
Set (or add if it's not present) the fs.inotify.max_user_watches parameter. Set this to the desired number of watches:
```cmd
fs.inotify.max_user_watches=1048576
```

Either reboot the system or execute the following command:
```cmd
sudo sysctl -p /etc/sysctl.conf
```
