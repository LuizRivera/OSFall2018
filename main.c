//
//  main.c
//  test
//
//  Created by Luiz Rivera on 10/1/18.
//  Copyright © 2018 Luiz Rivera. All rights reserved.
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

//converts a string that contains an int to a new string that strcmp() can use. numOfBytes tells the func the size of the new string
//For example-> target = "123" and numOfBytes = 5, the result would be-> "00123"
//remember to free the string that you sent into this function since you'll probs not use it again and to free this string
//also note every string you sent this must contain the same numOfBytes arg for strcmp to work
char * convertIntForStrcmp(char* target, int numOfBytes){
    
    char* final = (char*)malloc(numOfBytes * sizeof(char));
    int size = (int)strlen(target);
    int i,j;
                                    //zeroing out the newly created string
    for(i = 0; i < numOfBytes; i++){
        final[i] = '0';
    }
                                    //copy the arg string into the newly created string starting from right to left
    for(i = size -1 , j = numOfBytes -2 ; i >= 0; i --, j--){
        final[j] = target[i];
    }
    
    final[numOfBytes - 1] = '\0';
    return final;
}


int main(int argc, const char * argv[]) {
    
    char* target = "123456";
    char* target1 = "0";
    
    
    char* newTarget1 = convertIntForStrcmp(target, 10);
    char* newTarget2 = convertIntForStrcmp(target1, 10);
    
    int test = strcmp(newTarget1, newTarget2);
    
    printf("test: %d \n", test);

    return 0;
}
