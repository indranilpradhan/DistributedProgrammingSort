#include <stdio.h>
#include <string.h>
#include "mpi.h"
#include <fstream>
#include <bits/stdc++.h>
#include <sstream>
using namespace std;
typedef long long int ll;

void swap(ll* a, ll* b)
{
	ll temp = *a;
	*a = *b;
	*b = temp;
}

ll partition(ll arr[], ll low, ll high)
{
	ll pivotele = arr[high]; 
    ll min = low-1;
    ll max = high - 1; 
  
    while(true)
    {
      do
      {
        min++;
      }while(arr[min] < pivotele);
      
      while(max > 0 && arr[max] > pivotele)
      {
        max--;
      }

      if(min >= max)
        break;
      else
      {
        swap(&arr[min], &arr[max]);
      }
    }  
    swap(&arr[min], &arr[high]);  
    return min;
}

ll randompartition(ll arr[],ll low,ll high)
{
	ll random = low + (rand() % (high-low+1));
	swap(&arr[random], &arr[high]);
	return partition(arr, low, high);
} 

void quicksort(ll arr[], ll low, ll high)
{
	if(low<high)
	{
		ll pivot = randompartition(arr, low, high);
    	quicksort(arr,low,pivot-1);
      	quicksort(arr,pivot+1,high);
	}
}

int main( int argc, char **argv ) {
    int rank, numprocs;

    /* start up MPI */
    MPI_Init( &argc, &argv );

    MPI_Comm_rank( MPI_COMM_WORLD, &rank );
    MPI_Comm_size( MPI_COMM_WORLD, &numprocs );
    
    /*synchronize all processes*/
    MPI_Barrier( MPI_COMM_WORLD );
    double tbeg = MPI_Wtime();
    // cout<<"here"<<endl;
    /* write your code here */
    if(rank == 0) {
        ifstream infile(argv[1]);
        ofstream outfile(argv[2]);
        string fline;
        getline(infile, fline);
        ll N = stoll(fline,nullptr, 10);
        string sline;
        getline(infile,sline);
        ll unsorted_array[N];
        ll sorted_array[N];
        istringstream iss(sline);
        vector<string> result;
        ll i = 0;
        for(string s;iss>>s;)
        {
            unsorted_array[i] = stoll(s,nullptr,10);
            i++;
        }

        if(numprocs == 1)
        {
            for(ll i=0; i<N; i++)
            {
                sorted_array[i] = unsorted_array[i];
            }
            quicksort(sorted_array,0,N-1);
        }
        else
        {
            double a = double(N)/double(numprocs-1);
            int chunk_size = floor(a);
            // cout<<"chuk size "<<chunk_size<<endl;
            if(chunk_size == 0)
            {
                for(ll i=0; i<N; i++)
                {
                    sorted_array[i] = unsorted_array[i];
                }
                // cout<<"here "<<endl;
                for(int i=1; i<numprocs; i++)
                {
                    if(i == 1)
                    {
                        MPI_Send(&N,1,MPI_INT,i,0,MPI_COMM_WORLD);
                        MPI_Send(unsorted_array,N,MPI_LONG_LONG_INT,i,0,MPI_COMM_WORLD); 
                        // quicksort(sorted_array,0,N-1);
                        ll recv_chunk_size;
                        MPI_Recv(&recv_chunk_size, 1, MPI_LONG_LONG_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        MPI_Recv(sorted_array, recv_chunk_size, MPI_LONG_LONG_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    }
                    else
                    {
                        int t = 0;
                        ll t_ar[0];
                        MPI_Send(&t,1,MPI_INT,i,0,MPI_COMM_WORLD);
                        MPI_Send(t_ar,t,MPI_LONG_LONG_INT,i,0,MPI_COMM_WORLD); 
                        MPI_Recv(&t, 1, MPI_LONG_LONG_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        MPI_Recv(t_ar, t, MPI_LONG_LONG_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    }
                    
                }
            }
            else
            {
                ll temp_size = chunk_size;
                for(int i=1;i<numprocs;i++)
                {
                    // cout<<"temp size "<<temp_size<<endl;
                    if(temp_size < N)
                    {
                        // cout<<"start "<<chunk_size*(i-1)<<" i "<<i<<endl;
                        if(i == numprocs-1)
                        {
                            int remaining = N-chunk_size*(i-1);
                            // cout<<"here "<<remaining<<" start "<<chunk_size*(i-1)<<endl;
                            MPI_Send(&remaining,1,MPI_INT,i,0,MPI_COMM_WORLD);
                            MPI_Send(unsorted_array+chunk_size*(i-1),remaining,MPI_LONG_LONG_INT,i,0,MPI_COMM_WORLD);
                        }
                        else
                        {
                            MPI_Send(&chunk_size,1,MPI_INT,i,0,MPI_COMM_WORLD);
                            MPI_Send(unsorted_array+chunk_size*(i-1),chunk_size,MPI_LONG_LONG_INT,i,0,MPI_COMM_WORLD);   
                        }
                    }
                    else
                    {
                        int remaining = N-(temp_size -chunk_size);
                        // cout<<"remain "<<remaining<<endl;
                        MPI_Send(&remaining,1,MPI_INT,i,0,MPI_COMM_WORLD);
                        MPI_Send(unsorted_array+chunk_size*(i-1),remaining,MPI_LONG_LONG_INT,i,0,MPI_COMM_WORLD);
                    }
                    temp_size = temp_size+chunk_size;
                }
                int recv_chunk_size;
                ll a_size = 0;
                for(int i=1;i<numprocs;i++)
                {
                    MPI_Recv(&recv_chunk_size, 1, MPI_LONG_LONG_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    ll temp_sorted_array[recv_chunk_size];
                    MPI_Recv(temp_sorted_array, recv_chunk_size, MPI_LONG_LONG_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    if(a_size == 0)
                    {
                        for(int i=0; i< recv_chunk_size; i++)
                        {
                            sorted_array[i] = temp_sorted_array[i];
                        }
                    }
                    else
                    {
                        int k = 0, l = 0, p= 0;
                        ll temp_array[recv_chunk_size+a_size];
                        while(k<a_size && l<recv_chunk_size)
                        {
                            if(sorted_array[k]<temp_sorted_array[l])
                            {
                                temp_array[p] = sorted_array[k];
                                p++;
                                k++;
                            }
                            else
                            {
                                temp_array[p] = temp_sorted_array[l];
                                p++;
                                l++;
                            }
                        }
                        while(k<a_size)
                        {
                            temp_array[p] = sorted_array[k];
                            p++;
                            k++;
                        }
                        while(l<recv_chunk_size)
                        {
                            temp_array[p] = temp_sorted_array[l];
                            p++;
                            l++;
                        }

                        for(int i =0; i< a_size+recv_chunk_size; i++)
                        {
                            sorted_array[i] = temp_array[i];
                        }
                    }
                    a_size = a_size + recv_chunk_size;
                }   
            }
        }

        for(int i=0;i<N;i++)
            outfile<<sorted_array[i]<<" ";
    }
    else {
        int chunk_size;
        MPI_Recv(&chunk_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        ll received[chunk_size];
        // cout<<"before recve "<<chunk_size<<endl;
        MPI_Recv(received, chunk_size, MPI_LONG_LONG_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // cout<<"after recve "<<chunk_size<<endl;
	    quicksort(received,0,chunk_size-1);
        MPI_Send(&chunk_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
	    MPI_Send(received, chunk_size, MPI_LONG_LONG_INT, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Barrier( MPI_COMM_WORLD );
    double elapsedTime = MPI_Wtime() - tbeg;
    double maxTime;
    MPI_Reduce( &elapsedTime, &maxTime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD );
    if ( rank == 0 ) {
        printf( "Total time (s): %f\n", maxTime );
    }

    /* shut down MPI */
    MPI_Finalize();
    return 0;
}

