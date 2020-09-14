FROM nodered/node-red
USER node-red
EXPOSE 80
WORKDIR /usr/src/node-red
CMD ["node-red","-p", "80"]