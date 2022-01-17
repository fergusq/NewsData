#!/bin/sh

# pull the correct docker
sudo docker pull turkunlp/turku-neural-parser:latest-fi-en-sv-cpu

# save the cpu version image to tar
sudo docker save -o turkunlp.tar turkunlp/turku-neural-parser:latest-fi-en-sv-cpu

# give the docker file sufficient permissions
chmod +rwx turkunlp.tar

#move the tar file to the server 