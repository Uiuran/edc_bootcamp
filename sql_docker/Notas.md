# Docker

Build image and run service through exposed port  (use -d to detach process from terminal)

About volumes: builded docker images run by mounting volume in a place of operation system, when you stop de image and delete the volume will remain if you do not explicitly indicate that you are willing to delete the volume.

# Docker Compose

Orchestrate many services, each with one image, environment vars, ports from image:computer and volumes

Cheatsheet:

docker-compose -d <service-name>

