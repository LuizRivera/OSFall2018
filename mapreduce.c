#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <pthread.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

const char * app; //wordcount or sort
const char * impl; //procs or threads
int num_maps; //number of maps
int num_reduces; //number of reduces
const char * infile; //name of input file
const char * outfile; //name of output file

pthread_barrier_t map_barrier; //barrier used to wait for map threads to finish

//this marks the size of each index in the shared memory, so each map function has that much memory to play around with/
//helps sync issues since each reduce/map func has its own spot in the shared memory

long INDEX_SIZE;
long SHMEM_SIZE;

int SIZE = 1;

struct threadInput
{ 
   void* shareMem;
   int   threadID;
   char* partialBuffer;
};

//For word part you'd have to make the key the ascii value of that number and make the value the number since
//the reduce organization part sorts by the key

struct pair
{
	char* key;
	int value;
};
//Prints input array of pairs
//Debug function
void printPairArr( struct pair* pairArr ){
	printf("Printing Arr: \n");
	int i = 0;

	while(1){

		printf("Key  : %s\n",pairArr[i].key);
		printf("Value: %d\n",pairArr[i].value);
		
		printf("\n");
		i ++;

		if(pairArr[i].value == -1){
			break;
			}
		

	}

}

void* create_shared_memory(size_t size) {
  // Our memory buffer will be readable and writable:
  int protection = PROT_READ | PROT_WRITE;

  // The buffer will be shared (meaning other processes can access it), but
  // anonymous (meaning third-party processes cannot obtain an address for it),
  // so only this process and its children will be able to use it:
  int visibility = MAP_ANONYMOUS | MAP_SHARED;

  // The remaining parameters to `mmap()` are not important for this use case,
  // but the manpage for `mmap` explains their purpose.
  return mmap(NULL, size, protection, visibility, 0, 0);
}

//will write the input pair array to the location marked by the threadID
//Note the pair array end node must be an end node aka the key must be -1 to mark the end of the array
//example-> pairArr = [ (1,"hello"), (2,"whats good"), (-1,"end") ]
void writeToSharedMem(void* shareMem, struct pair * pairArr, int threadID){

	int x = threadID * INDEX_SIZE;			//finding the index to write to
	int y = 0;
	int i = 0;

	while(1){

		memcpy(shareMem + y + x, &(pairArr[i]), sizeof(struct pair));
		
		y+= sizeof(struct pair);	//incr the next position in the pair array store in share memory

		if(pairArr[i].value == -1)	//marks the end of the pair array
		{

			break;
		}

		i++;
		}	

}
//will print all pairs located in the index marked by the threadID
//Debug function
void printContentsShareMem(int threadID, void* shareMem){
	
	int x = threadID * INDEX_SIZE;
	int y = 0;


	printf("\nContents of thread: %d\n",threadID);
	while(1){
		struct pair* pairArr = (struct pair*)(shareMem + x + y); //have to incr shareMem first before type casting
									//cant type cast first then treat as array for whatever reason

		if(pairArr->value == -1)	//marks the end of the pair array
		{
			break;
		}
		
		printf("Key  : %s\n", pairArr->key);
		printf("Value: %d\n", pairArr->value);
		printf("\n");
		

		y += sizeof(struct pair);
		
		}

}

//returns a COPY of a pair array located by the threadID
//thus you gonna have to free() this copy after your done with it
struct pair* getCopyOfPairArr( void* shareMem, int threadID){

	
	int x = threadID * INDEX_SIZE;			//finding the index to write to
	int y = 0;
	int size = 0;
	int i = 0;
						//have to first find the size of the array in order to allocate enough space
	while(1){
		size ++;
		struct pair* pairArr = (struct pair*)(shareMem + x + y);
		
		if(pairArr->value == -1){
			break;
				}

		y += sizeof(struct pair);
		
		}

	struct pair* pairArrCopy = (struct pair*)malloc( size * sizeof( struct pair) );
	
	y = 0;
						//once we have enough space, we can then go thru again and copy its contents
	while(1){
		
		struct pair* pairArr = (struct pair*)(shareMem + x + y);
		
		pairArrCopy[i].key = pairArr->key;
		pairArrCopy[i].value = pairArr->value;

		if(pairArr->value == -1){
			break;
				}

		y += sizeof(struct pair);
		i ++;
		
		}

	return pairArrCopy;


}

//just a function to check if your a the cap of the array, if so then extends is by times 2
struct pair * checkArrlist(struct pair* pairArr, int current){	
				//getting size of array
	int size = SIZE;

	if( current >= size ){
					//doubles the size of the array
		struct pair * nwPairArr = (struct pair*)malloc( (size * 2) * sizeof(struct pair));
		memcpy(nwPairArr, pairArr, (size) * sizeof(struct pair) );
		SIZE = size * 2;
		free(pairArr);

		return nwPairArr;
			}

	else{
		return pairArr;
	}
	
}


//will merge all of the pair arrs located in the shared memory in order and write a new pair arr back to the first index in the shared memory
//max threadID tells the system how many different indexes to merge from
void mergeShareMem(void* shareMem, int maxThreadID){
	
	char* minKey;
	int i, size, minValue,current;
	size = 1;
	current = 0;

	struct pair* mergePairArr = (struct pair*)malloc( size * sizeof(struct pair) );
	
		
								//this holds the current position for each thread
	int* positionArr = (int*)malloc( maxThreadID * sizeof(int)); 
	
								//making the starting position for each thread zero
	for(i = 0; i < maxThreadID ; i ++)				
	{
		positionArr[i] = 0;
	}
	while(1)
	{
										//making the def min the first pair in the first thread
	      struct pair* pairArr = (struct pair*)(shareMem + positionArr[0]);
	      minValue = pairArr->value;
	      minKey   = pairArr->key;
		
								//have to first find the most min value
		for( i = 0; i < maxThreadID ; i ++)
		{
		
			int x = i * INDEX_SIZE; 				//calcing the index
									//getting the pair at the current ith index
			struct pair* pairArr = (struct pair*)(shareMem + x + positionArr[i]);	
			if(pairArr->value == -1){
				continue;
						}
			
			if( 0 > strcmp(pairArr->key, minKey) ){
				minValue = pairArr->value;
				minKey   = pairArr->key; 
					}
			if(minValue == -1){			//if the current min is a end node then change it to the current node
				minValue = pairArr->value;
				minKey   = pairArr->key; 
				}
			
		}
	
							//if the mid is the end node, then your done
		if(minValue == -1){
			break;
				}
		
							//Now that we have the mind value we have to go back and see which min values
							//match, and if they match add them to the merge arr and incr the position for that thread
		
		for(i = 0; i < maxThreadID; i ++)
		{
		   int x = i * INDEX_SIZE;
		   struct pair* pairArr = (struct pair*)(shareMem + x + positionArr[i]);
		
		   if(pairArr->value == -1)
			{
				continue;
			}
			
		   if(0 == strcmp(pairArr->key, minKey )  ) //found a match min so add it the merge arr
			{
		        mergePairArr = checkArrlist(mergePairArr, current);
			mergePairArr[current].key   = pairArr->key;
			mergePairArr[current].value = pairArr->value;
			
			current += 1;
								//now have to incr the posistion of that index
			
			positionArr[i] += sizeof(struct pair);
			
			
			}
			
			
		}
							

	}
	
	mergePairArr = checkArrlist(mergePairArr, current);
							//giving the merge arr a end node
	mergePairArr[current].key  = "end";
	mergePairArr[current].value = -1;
							//writing the new merged arr back to the first index in shared memory 
	writeToSharedMem(shareMem, mergePairArr, 0);
	free(mergePairArr);
	free(positionArr);

}
//function divides up the pair array located in the first index of shared memory and writes matching pairs to indiv indexes with the last
//index having the rest of the pairs. Takes in shared memory pointer and the max number of reduce threads
void writeBackOrganize(void* shareMem, int maxThreadID){

						//getting the sorted pair array
	struct pair* organizeArr = getCopyOfPairArr(shareMem, 0);

	int currentThread = 0;
	int currentPos = 1;
	int currentPairArr = 1;
	SIZE = 1;
						//the array that holds matching pairs to be writen to shared memory
	struct pair* pairArr = (struct pair *)malloc( sizeof(struct pair));

						//making the first entry in the org array the first entry in the matching array
	pairArr[0].key   = organizeArr[0].key;
	pairArr[0].value = organizeArr[0].value;
	
	while(1){			
						//if were on the last reduce thread then we just have to write the rest of the org array to that
						//threads spot in shared memory
		if(currentThread == (maxThreadID -1)){
			writeToSharedMem(shareMem, organizeArr += (currentPos -1), currentThread );
	//		free(pairArr);
	//		free(organizeArr);
			return;
						}
						//if were at the limit of the org array thus the matching pair array is written to shared memory
		if(organizeArr[currentPos].value == -1){
			pairArr = checkArrlist(pairArr, currentPos);
			pairArr[currentPairArr].key   = organizeArr[currentPos].key;
			pairArr[currentPairArr].value = organizeArr[currentPos].value;

			writeToSharedMem(shareMem, pairArr, currentThread );
	//		free(pairArr);
	//		free(organizeArr);

					     //since were at the limit of shared memory we have to write the def end node to the remaining reduces
			int i;
			for(i = currentThread + 1; i < maxThreadID; i += 1){
				pairArr = (struct pair*)malloc(sizeof(struct pair));
				pairArr[0].value = -1;
				pairArr[0].key = "end";
				writeToSharedMem(shareMem, pairArr, i);
	//			free(pairArr);
					}	
			return;  
			}
					     //the next node on the org array matching the matching pair thus we add it to that array
		if( strcmp(pairArr[0].key, organizeArr[currentPos].key )== 0){
			pairArr = checkArrlist(pairArr, currentPairArr);
			pairArr[currentPairArr].key   = organizeArr[currentPos].key;
			pairArr[currentPairArr].value = organizeArr[currentPos].value;
			currentPairArr += 1;
			}

					//if the next pair doesnt match so we write the matching pair array to its location and start anew
		else{ 
			pairArr = checkArrlist(pairArr, currentPos);
			pairArr[currentPairArr].value   = -1;
			pairArr[currentPairArr].key = "end";
			writeToSharedMem(shareMem, pairArr, currentThread );
			

			currentThread += 1;
			currentPairArr = 1;
			pairArr[0].key   = organizeArr[currentPos].key;
			pairArr[0].value = organizeArr[currentPos].value;
		}
		currentPos += 1;
	}
}


//sorts the array of pairs based on their key values on the index based on the threadID and writes the newly sorted array back to shared memory
void sortPairArr(void* shareMem, int threadID)
{
   
   int  value, j;
   int size = 0;
   int i = 0;
   char* key;
   	
   struct pair * pairArr = getCopyOfPairArr(shareMem, threadID);

  					 //getting the size of the pairArr
	while(1){
		size ++;
		if(pairArr[i].value == -1){
			break;
			}
		i ++;

		}

  					//sorting the array using standard insertion sort, not the best but the easiest way to do it
   for (i = 1; i < (size -1); i++)	//(size -1) b/c we dont want to include the end node in sorting
   {
       key = pairArr[i].key;
       value = pairArr[i].value;

       j = i-1;
 
      
       while (j >= 0 && (0 < strcmp(pairArr[j].key, key)) )  
       {
           pairArr[j+1].key = pairArr[j].key;
	   pairArr[j+1].value = pairArr[j].value;

           j = j-1;
       }

	   
	   		       
       pairArr[j+1].key = key;
       pairArr[j+1].value = value;
   }

					//finished sorting now writing the new array of pairs back to the shared memory
   writeToSharedMem(shareMem, pairArr, threadID );

}

void * map_sort(void * threadArg){
    struct threadInput * input  = (struct threadInput *)threadArg;
   // printf("Thread: %d got here\n", (*input).threadID);
   // printf("%s\n", (*input).partialBuffer);
    char * buffer = (*input).partialBuffer;
    void * shmem = (*input).shareMem;
    int threadID = (*input).threadID;
    int i;
    int start = 0, end = 0, totalNumbers = 0, pairPos = 0;
    int keyPos = 0;
    int rule = 0;
    for(i = 0; i < strlen(buffer); i++){
        if(buffer[i] >= '0' && buffer[i] <= '9'){
        	rule = 0;
            continue;
        }
        else {
        	if(rule == 1){
        		continue;
        	}
        	rule = 1;        	
            totalNumbers++;
        }
    }

    struct pair* pairArr = (struct pair *)malloc( (totalNumbers+1) * sizeof(struct pair));
    char * key;
	printf("total numbers: %d\n", totalNumbers);
    while(start < strlen(buffer)){
        if(buffer[start] >= '0' && buffer[start] <= '9'){
            start++;
        }
        else {
            key = malloc(start-end+1);
            for(i = end; i < start; i++){ //copy current string into key
                key[keyPos] = buffer[i];
                keyPos++;
            }
            keyPos = 0; //reset keyPos for next key
            key[start-end] = '\0';
            int value = atoi(key);
            //modify key to contain the correct ASCII values so that strcmp will work
            for(i = 0; i < strlen(key); i++){
                /*
                char * ptr = malloc(1);
                ptr[0] = key[i];
                int place = atoi(ptr);
                key[i] = (char)place;
                free(ptr);
                */
                key[i] = key[i] - '0';
            }
            //add key and value to pairArr
            pairArr[pairPos].value = value;
            pairArr[pairPos].key = key;
            
            pairPos++;

            //move start and end to next token
           // if(start+1 < strlen(buffer)) {
           	//	printf("This is the old start: '%c'\n", buffer[start]);
                end = start+2;
                start = end;
          //      printf("This is the new start: '%c'\n", buffer[start]);
           // }
        }
    }
    //need to save last number somehow
    pairArr[totalNumbers].key = "end";
    pairArr[totalNumbers].value = -1;
    
    
//    printPairArr(pairArr);
    
    writeToSharedMem(shmem, pairArr,threadID);
    pthread_barrier_wait(&map_barrier);    

    return NULL;
}


int main(int argc, char ** argv){


    if(argc < 13){
        printf("Error: not enough arguments\n");
        exit(1);
    }
    app = argv[2];
    
    //basic error check
    if( (strcmp(app,"wordcount") != 0) && (strcmp(app,"sort") != 0) ){
        printf("Error: Invalid app name\n");
        exit(1);
    }
    impl = argv[4];

    //basic error check
    if( (strcmp(impl,"procs") != 0) && (strcmp(impl,"threads") != 0) ){
        printf("Error: Invalid argument\n");
        exit(1);
    }
    num_maps = atoi(argv[6]);
    num_reduces = atoi(argv[8]);
    infile = argv[10];
    outfile = argv[12];

    pthread_t map_threads[num_maps];
   
    int i,j;

    FILE *f = fopen(infile, "rb");
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    SHMEM_SIZE = fsize * 20;
    INDEX_SIZE = SHMEM_SIZE / num_maps;

    fseek(f, 0, SEEK_SET);

    char * buffer = malloc(fsize+1); //error check
    fread(buffer,fsize,1,f); //error check
    fclose(f);

    buffer[fsize] = 0;
    int * delimArr = (int*)malloc(sizeof(int) * (num_maps +1));
    
    //loop through buffer and splits into num_maps -1, then the last thread will handle the rest
    // if nummaps == 1 no need for delim
    
    int position;
    int delimPos = 1;
    delimArr[0] = -1;
    
    if(num_maps == 1){
    	position = fsize - 1; // -1 because the size is 1 greater than the position
    	delimArr[delimPos] = position;
    } else { //if num_maps(# of threads) > 1
    	int size = fsize/num_maps; // gets the ideal sizes of each thread
    	int i;
    	for(i = 0; i < num_maps; i++){
    		if(i == num_maps - 1){ //this is created for the last iteration of the loop
    			delimArr[delimPos] = fsize - 1;
    			break;
    		}
    		position = ((i+1) * size) - 1; //gets the position at the (i+1)th thread    	
    		while(buffer[position] != '\n'){
    			position--;
    		}
//    		printf("At position %d is this char %c\n", position, buffer[position]);
    		delimArr[delimPos] = position;
    		delimPos++;	
    	}
    }

    void* shmem = create_shared_memory((size_t)SHMEM_SIZE);

    if(strcmp(impl,"threads") == 0){

        int pos;
        struct threadInput * arg;
        char * buffSplit;
        int prev = 0;
        pthread_barrier_init(&map_barrier,NULL,num_maps+1);
        for(i = 0; i < num_maps; i++){ //creating threads 
           arg = malloc(sizeof(struct threadInput));
           (*arg).threadID = i;
           (*arg).shareMem = shmem;
           buffSplit = (char*) malloc(delimArr[i+1]-prev+1);
           pos = 0;
           for(j = prev; j < delimArr[i+1]; j++){
              buffSplit[pos] = buffer[j];
              pos++;
           }
           prev = delimArr[i+1]+1;
           (*arg).partialBuffer = buffSplit;
           if(strcmp(app,"sort") == 0){
//           	  printf("THIS IS THE ARG PARTIALBUFF: \n%s\n", (*arg).partialBuffer);
              pthread_create(&map_threads[i], NULL, map_sort, (void*)arg);
           }  
        }
        pthread_barrier_wait(&map_barrier);
        for(i = 0; i < num_maps; i++){
            pthread_join(map_threads[i], NULL);
        }
        pthread_barrier_destroy(&map_barrier);
        for(i = 0; i < num_maps; i++){
            sortPairArr(shmem, i);
        }
        mergeShareMem(shmem,num_maps);
//        writeBackOrganize(shmem, num_maps);
       
        printContentsShareMem(0,shmem);
    }
    return 0;
}
