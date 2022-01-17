#!/bin/sh

# create image from the dockerfile
docker build . --tag finer:latest
# Save the image to tar
docker save -o finer.tar finer:latest

# give the docker file sufficient permissions
chmod +rwx finer.tar

#move the tar file to the server 