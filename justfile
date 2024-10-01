
watch-test:
    watchexec --restart --verbose --clear --wrap-process=session --stop-signal SIGTERM --exts gleam --watch ./ -- "gleam test"

