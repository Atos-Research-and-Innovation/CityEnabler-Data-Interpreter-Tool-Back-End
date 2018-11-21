# Docker info

This Dockerfile is to create an image of the DIT Back End.   

This Dockerfile downloads the code from the CEDUS bitbucket repository.   

```
 docker build -t ditbackend .
```

To launch the container:   
   
```
docker run -d -p 0.0.0.0:8000:8000 ditbackend
```


