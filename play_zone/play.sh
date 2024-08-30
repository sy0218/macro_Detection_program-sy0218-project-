#!/usr/bin/bash
/data/keyboard_sc/play_get_log.py &

PID=$(ps aux | grep 'play_get_log.py' | grep -v grep | awk '{print $2}')

# trap는 특정 시그널시 지정 명령어 실행하는 기능
trap "kill $PID" TERM
tail -f /dev/null
