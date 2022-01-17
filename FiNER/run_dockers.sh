#!/bin/sh

# load the image
sudo docker load --input finer.tar

# run the image
sudo docker run -d -p 3000:3000 finer

# load another image
sudo docker load --input turkunlp.tar

# and run it
sudo docker run -d -p 15000:7689 turkunlp/turku-neural-parser:latest-fi-en-sv-cpu server fi_tdt parse_plaintext 
