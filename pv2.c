#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <pthread.h>
#include <string.h>
//this marks the size of each index in the shared memory, so each map function has that much memory to play around with/
//helps sync issues since each reduce/map func has its own spot in the shared memory

#define INDEX_SIZE 200

int SIZE = 1;
#define keySize 20
struct threadInput
{ 
   void* shareMem;
   int   threadID;
   char* wordArray;
};

//For word part you'd have to make the key the ascii value of that number and make the value the number since
//the reduce organization part sorts by the key

struct pair
{
	char key[keySize];
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
		
		//pairArrCopy[i].key = pairArr->key;
                strcpy(pairArrCopy[i].key, pairArr->key);/////new
		pairArrCopy[i].value = pairArr->value;

		if(pairArr->value == -1){
			break;
				}

		y += sizeof(struct pair);
		i ++;
		
		}

	return pairArrCopy;


}

//just a function to check if your at the cap of the array, if so then extends is by times 2
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
	//pairArr[0].key   = organizeArr[0].key;
        strcpy(pairArr[0].key , organizeArr[0].key); //new
	pairArr[0].value = organizeArr[0].value;
	
	while(1){			
						//if were on the last reduce thread then we just have to write the rest of the org array to that
						//threads spot in shared memory
		if(currentThread == (maxThreadID -1)){
			writeToSharedMem(shareMem, organizeArr += (currentPos -1), currentThread );
			free(pairArr);
			free(organizeArr);
			return;
						}
						//if were at the limit of the org array thus the matching pair array is written to shared memory
		if(organizeArr[currentPos].value == -1){
			pairArr = checkArrlist(pairArr, currentPos);
			//pairArr[currentPairArr].key   = organizeArr[currentPos].key;
                         strcpy( pairArr[currentPairArr].key, organizeArr[currentPos].key);   ///new

			pairArr[currentPairArr].value = organizeArr[currentPos].value;

			writeToSharedMem(shareMem, pairArr, currentThread );
			free(pairArr);
			free(organizeArr);

					     //since were at the limit of shared memory we have to write the def end node to the remaining reduces
			int i;
			for(i = currentThread + 1; i < maxThreadID; i += 1){
				pairArr = (struct pair*)malloc(sizeof(struct pair));
				pairArr[0].value = -1;
				pairArr[0].key[0] = 'e';    //new
                                pairArr[0].key[1] = 'd';
                                pairArr[0].key[2] = '\0';

				writeToSharedMem(shareMem, pairArr, i);
				free(pairArr);
					}	
			return;  
			}
					     //the next node on the org array matching the matching pair thus we add it to that array
		if( strcmp(pairArr[0].key, organizeArr[currentPos].key )== 0){
			pairArr = checkArrlist(pairArr, currentPairArr);
			//pairArr[currentPairArr].key   = organizeArr[currentPos].key;
                         strcpy( pairArr[currentPairArr].key, organizeArr[currentPos].key); //new
			pairArr[currentPairArr].value = organizeArr[currentPos].value;
			currentPairArr += 1;
			}

					//if the next pair doesnt match so we write the matching pair array to its location and start anew
		else{ 
			pairArr = checkArrlist(pairArr, currentPos);
			pairArr[currentPairArr].value   = -1;
			//pairArr[currentPairArr].key = "end";
                        //pairArr[currentPairArr].key[0] = 'e';    //new
                       // pairArr[currentPairArr].key[1] = 'd';
                       // pairArr[currentPairArr].key[2] = '\0';
                         strcpy( pairArr[currentPairArr].key, "end"); 
			writeToSharedMem(shareMem, pairArr, currentThread );
			

			currentThread += 1;
			currentPairArr = 1;
			//pairArr[0].key   = organizeArr[currentPos].key;
                        strcpy( pairArr[0].key, organizeArr[currentPos].key);   //new

			pairArr[0].value = organizeArr[currentPos].value;
		}
		currentPos += 1;
	}
}

//will merge all of the pair arrs located in the shared memory in order and write a new pair arr back to the first index in the shared memory
//max threadID tells the system how many different indexes to merge from
void mergeShareMem(void* shareMem, int maxThreadID){
	
	//char* minKey;
	int i, size,current,minValue;
	size = 1;
	current = 0;
        char minKey[keySize];
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
              strcpy( minKey, pairArr->key);   ///new
	      minValue = pairArr->value;
	     // minKey   = pairArr->key;
		
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
				//minKey   = pairArr->key; 
                                strcpy( minKey, pairArr->key); //new
					}
			if(minValue == -1){			//if the current min is a end node then change it to the current node
				minValue = pairArr->value;
				//minKey   = pairArr->key; 
                                 strcpy( minKey, pairArr->key);
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
			//mergePairArr[current].key   = pairArr->key;
                        strcpy( mergePairArr[current].key , pairArr->key);  //new
			mergePairArr[current].value = pairArr->value;
			
			current += 1;
								//now have to incr the posistion of that index
			
			positionArr[i] += sizeof(struct pair);
			
			
			}
			
			
		}
							

	}
	
	mergePairArr = checkArrlist(mergePairArr, current);
							//giving the merge arr a end node
	//mergePairArr[current].key  = "end";
        strcpy( mergePairArr[current].key , "end");  //new
	mergePairArr[current].value = -1;
							//writing the new merged arr back to the first index in shared memory 
	writeToSharedMem(shareMem, mergePairArr, 0);
	free(mergePairArr);
	free(positionArr);

}

//sorts the array of pairs based on their key values on the index based on the threadID and writes the newly sorted array back to shared memory
void sortPairArr(void* shareMem, int threadID)
{
   
   int  value, j;
   int size = 0;
   int i = 0;
  // char* key;
    char key[keySize];	
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
      // key = pairArr[i].key;
      strcpy( key, pairArr[i].key); //new
      value = pairArr[i].value;

       j = i-1;
 
      
       while (j >= 0 && (0 < strcmp(pairArr[j].key, key)) )  
       {
           //pairArr[j+1].key = pairArr[j].key;
             strcpy( pairArr[j+1].key, pairArr[j].key);  //new
	   pairArr[j+1].value = pairArr[j].value;

           j = j-1;
       }
       //pairArr[j+1].key = key;
         strcpy( pairArr[j+1].key, key); //new
       pairArr[j+1].value = value;
   }

					//finished sorting now writing the new array of pairs back to the shared memory
   writeToSharedMem(shareMem, pairArr, threadID );

}


int BiggestNumByte = 10;

void writePairsToFile(char* filename, int numOfReduces, void* shareMem){
    printf("w1\n");
    FILE *fp;
    int i;
    int j;
    fp = fopen( filename, "w" );

     printf("w2\n");
    if (fp == NULL)
    {
        printf("Error opening file!\n");
        exit(1);
    }
 printf("w3\n");
    for(i = 0; i < numOfReduces; i++){

    struct pair* pairArr = getCopyOfPairArr( shareMem, i);
     
    j = 0;
  printf("yooo\n");
    while(1){
       
       if(pairArr[j].value == -1){break;}
        
      char* snum = (char*)malloc( BiggestNumByte * sizeof(char));
      sprintf(snum, "%d", pairArr[j].value);    
      fprintf(fp, pairArr[j].key );
      fprintf(fp, " ");
      fprintf(fp, snum );
      
      

      fprintf(fp,"\n");
      free(snum);
       j++;
     
        }
     free(pairArr);
    } 
   
    
    fclose(fp);
}
main(){

	void* shmem = create_shared_memory(2000);
	
	struct pair* pairArr = (struct pair *)malloc( 6 * sizeof(struct pair));
	
	pairArr[0].value = 2;
	//pairArr[0].key= "z";
       strcpy(pairArr[0].key, "z");  //new

	pairArr[1].value = 2;
	///pairArr[1].key = "z";
             strcpy(pairArr[1].key, "z");  //new 


	pairArr[2].value = -1;
	//pairArr[2].key = "end";
            strcpy(pairArr[2].key, "end");  //new
	

	writeToSharedMem(shmem, pairArr, 0 );
	
	

	//pairArr[0].key = "end";
	pairArr[0].value = -1;

           strcpy(pairArr[0].key, "end");  //new


	writeToSharedMem(shmem, pairArr, 1 );

	pairArr[0].value = 2;
	//pairArr[0].key = "x";

     strcpy(pairArr[0].key, "x");  //new

	pairArr[1].value = 2;
	//pairArr[1].key = "x";
     strcpy(pairArr[1].key, "x");  //new

	pairArr[2].value = 2;
	//pairArr[2].key = "z";
     strcpy(pairArr[2].key, "z");  //new

	pairArr[3].value = 2;
	//pairArr[3].key = "x";
     strcpy(pairArr[3].key, "x");  //new

	pairArr[4].value = -1;
	//pairArr[4].key = "end";
	 strcpy(pairArr[4].key, "end");  //new

	writeToSharedMem(shmem, pairArr, 2 );

	sortPairArr(shmem, 0);
	sortPairArr(shmem, 1);
	sortPairArr(shmem, 2);
	
	printContentsShareMem(0,shmem);
	printContentsShareMem(1,shmem);
	printContentsShareMem(2,shmem);

	
	mergeShareMem(shmem, 3);
	
	
	printf("\nMerged: ");
	printContentsShareMem(0,shmem);	


	printf("Write Back Org: \n");
	writeBackOrganize(shmem, 3);

	printf("printing\n");
	printContentsShareMem(0,shmem);
	printContentsShareMem(1,shmem);
	printContentsShareMem(2,shmem);

     printf("bout to write to file\n");
      writePairsToFile("test.txt", 3, shmem); 
   
	return 0;
}















