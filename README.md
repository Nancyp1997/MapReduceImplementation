# MapReduceImplementation
Implementing Map reduce paper in Golang. Dealing with race conditions, worker failures.<br/>
In this repo, I am trying to implement the word count problem using Map reduce approach. <br/>Initially master reads the text files provided at run time.
<br/>After the master starts, if any worker thread requests for map or reduce task, it checks its available(not yet started) map or reduce tasks. <br/>
Master wouldn't be handing out the reduce tasks unless all the map tasks are complete. <br/>Hence if worker accidentally requests reduce task before map task,
then it still receives a map task, which we keep track via custom Task struct.<br/><br/>
## For map tasks:
Each worker would be given a unique ID at init. Each map task has to be divided into 'nReduce' number of reduce tasks. <br/>
Worker applies map function to the content read from the file it receives from master and writes the intermediate output of format (<word>, 1) to intermediate file 
which are named as mr-<workerID>-<reduceID>. <br/>
Once the task is initiated, the master sleeps for 10 seconds and checks if the custom struct "Task"'s status changes to complete. <br/>If not, the task is again marked as incomplete and would be later given to next worker.<br/>
If the master receives ACK within 10 seconds, it marks the task as complete and creates Task structs for all the intermediate files. <br/>All these 
newly created Tasks are added to reduce queue of master. <br/>
Once all map tasks are done, reduce stage starts.<br/><br/>
## For reduce tasks:
Since single file is processed by single worker, we group the reduce tasks by workerID. <br/><br/>
## Final done() stage
Once the reduce queue in the master is empty, the master exits.<br/><br/>

# To do
Check if worker's ACK is received 10 secs later.<br/><br/>

#To run:
Open atleast 2 terminal tabs, one for coordinator.go & rest for different workers(worker.go)<br/>
go build -race -buildmode=plugin ../mrapps/wc.go<br/>
go run -race mrcoordinator.go pg-*.txt (To start master)<br/>
go run -race mrworker.go wc.so (To start single worker)<br/><br/>

# To test:
bash test-mr.sh<br/>
