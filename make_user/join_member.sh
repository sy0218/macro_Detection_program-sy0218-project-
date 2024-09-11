#!/usr/bin/bash

if [ $# -ne 1 ]; then
	echo "Usage: $0 <user_name>"
	exit 1
fi

user_name=$1
echo "user_making start : $user_name"
useradd -m -g game_user $user_name
usermod -aG input $user_name
su - $user_name -c "pip install kafka-python"
echo "user_making completion : $user_name"
