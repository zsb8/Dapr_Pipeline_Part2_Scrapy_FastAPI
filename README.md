This is the part2 of the Pipline. It use FastAPI to get the scraping data from the Pipeline part1.
Compare the web data and jobs collection. Insert the new data and modify the expired data in jobs.
Then Compare the jobs and postedjobs. Insert the new data and modify the expired data in postedjobs.
It tested OK with noahs and da-services on K8S with Dapr.
You should run these programs together: WB_DA_API_services, WB_noahs_doves and WB_pipelines_FastAPI_K8S_DAPR.    

![image](https://user-images.githubusercontent.com/75282285/150705537-a7460908-22c1-4d0f-a511-396cb4fc4739.png)


# Deploy on K8S with Dapr
```
kubectl apply -f da_pipelines_dapr.yaml 
```
![image](https://user-images.githubusercontent.com/75282285/150704611-9919144d-e88d-4b2f-a720-69b86eefd142.png)


![image](https://user-images.githubusercontent.com/75282285/150704632-8201d74e-bf81-4c4a-9d78-aa20cd3d8be7.png)


# Test on Dapr

![image](https://user-images.githubusercontent.com/75282285/150704701-35bdb187-6695-4741-ad54-52b8ebe036bc.png)

~~~
kubectl port-forward da-pipelines-59b8c5b875-6qrjw  8070:3500
~~~

```
curl http://127.0.0.1:8070/v1.0/invoke/da-pipelines/method/
````
![image](https://user-images.githubusercontent.com/75282285/150704586-2085035a-79cb-4d97-bbcc-2eb84fd1d223.png)

# See the log on K8S
```
kubectl logs da-pipelines-59b8c5b875-6qrjw  -c da-pipelines
```
![image](https://user-images.githubusercontent.com/75282285/150704684-1b5ed55b-ae83-452f-a002-4ec0fa5fe2b4.png)

 
