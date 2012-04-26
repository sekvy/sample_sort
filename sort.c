 /*
An c implementation of parallel sorting by regular sampling (PSRS)
Authors:
Simon Ekvy & Johan Anderholm

May knock your socks off
*/
#include <string.h> 
#include <assert.h>
#include <limits.h>
#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/times.h>
#include <sys/time.h>
#include <unistd.h>

//single threaded is faster if SIZE < N_THREADS*1000  (roughly)
//SIZE must be greater than N_THREADS 
#define N_THREADS 4
#define SIZE 20000000

#ifdef DEBUG
#define D if(1)
#else
#define D if(0)
#endif

static double sec(void)
{
    struct timeval tim;
    gettimeofday(&tim, NULL);
    return tim.tv_sec+(tim.tv_usec/1000000.0);
}

//it returns the smallest index of which the number is bigger than or equal to the key
int binarySearch(double key, double* array, int length)
{
    int front=0, end=length-1;
    if (key>array[end]) {
        return length;
    }
    if (key<array[front]) {
        return 0;
    }

    int pos=(front+end+1)/2;;
    while (front<=end) {
        if (key>array[pos]) {
            front=pos+1;
        } else {
            if (key<array[pos]) {
                end=pos-1;
            } else {
                break;
            }
        }
        pos=(front+end+1)/2;
    }

    return pos;
}

struct thread_args {
    double*		base;		// Array to sort.
    size_t		n;	// Number of elements in base.
    size_t		s;	// Size of each element.
    int		(*cmp)(const void*, const void*); // Behaves like strcmp
    int 	id;
    double*		a;
};

struct partition {
    double*		base;		// Array to sort.
    size_t		n;	// Number of elements in base.
};

pthread_mutex_t init_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t init_lock2 = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond2 = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond3 = PTHREAD_COND_INITIALIZER;

void* sort(void *threadargs)
{
    static int pass = 0;
    static int pass2 = 0;
    static int pass3 = 0;
    static int sorted = 0;
    static double pivots[N_THREADS*N_THREADS];
    static double pivots_sel[N_THREADS-1];
    static struct partition partitions[N_THREADS*N_THREADS];
    static struct partition* s_partitions[N_THREADS*N_THREADS]; //sorted partitions
    static int p_size[N_THREADS]; //size of each sorted sublist
    struct thread_args *args;
    args = (struct thread_args *) threadargs;
    int rel = args->id*N_THREADS;  //relative position
    int id = args->id; //this threads id
    double* s_list; //sorted sublist
    int i;
    int j;

    qsort(args->base, args->n, sizeof(double), args->cmp);

    //0, n/P^2 , 2n/P^2 , . . . , (P − 1)(n/P^2 )
    for(i = 0; i < N_THREADS; i++) {
        pivots[rel+i] = args->base[i*args->n/N_THREADS];
    }

    //wait for threads to sort their sublist and gather sample pivots
    //then let the last thread to finish sort and select N_THREADS-1 pivots
    //-------------------------------
    //-------------------------------
    pthread_mutex_lock(&init_lock);
    pass++;
    if(pass==N_THREADS) {
        qsort(&pivots, N_THREADS*N_THREADS, sizeof(double), args->cmp);
        //i*p + p/2 -1
        for(i = 0; i < N_THREADS-1; i++) {
            pivots_sel[i] = *(pivots + ((i+1)*N_THREADS + N_THREADS/2 -1) );
        }
        sorted = 1;
        pass = 0;
    }

    D printf("ID: %d done with qsort\n", id);

    while (sorted != 1)
        pthread_cond_wait(&cond, &init_lock);
    pthread_mutex_unlock(&init_lock);
    pthread_cond_broadcast(&cond);

    //create the partitions using the selected pivots
    //-------------------------------
    //-------------------------------
    int pos = 0;
    int last_pos = 0;
    for(i = 0; i < N_THREADS-1; i++) {
        partitions[rel+i].base = args->base + pos;
        pos = binarySearch(pivots_sel[i], args->base, args->n);
        partitions[rel+i].n = pos - last_pos;
        last_pos = pos;
    }
    partitions[rel+N_THREADS-1].base = args->base + pos;
    partitions[rel+N_THREADS-1].n = args->n - last_pos;

    //all to all communication, sort partitions
    //also caluclate total size of the sublist
    //-------------------------------
    for(i = 0; i < N_THREADS; i++) {
        s_partitions[i*N_THREADS+id] = &partitions[rel+i];
        __sync_fetch_and_add(&p_size[i], (&partitions[rel+i])->n); //atomic add
    }

    pthread_mutex_lock(&init_lock2);
    pass2++;
    while (pass2 != N_THREADS)
        pthread_cond_wait(&cond2, &init_lock2);
    pthread_mutex_unlock(&init_lock2);
    pthread_cond_broadcast(&cond2);

    D printf("ID: %d about to merge\n", id);

    //sort the sublist
    //-------------------------------
    //-------------------------------
    s_list = malloc(sizeof(double) * p_size[id]);
    double* parts[N_THREADS];
    for (i = 0; i < N_THREADS; ++i) {
        parts[i] = s_partitions[rel+i]->base;
    }

    double min;
    int mini;

    D printf("ID: %d entering merge for loop\n", id);

    for (i = 0; i < p_size[id]; ++i) {
        for (j = 0; !parts[j]; ++j);

        min = *parts[j];
        mini = j;
        for(; j < N_THREADS; ++j) {
            if (parts[j] != NULL && *parts[j] < min) {
                min = *parts[j];
                mini = j;
            }
        }
        struct partition* part = s_partitions[rel+mini];

        assert(parts[mini] != NULL);
        parts[mini]++;
        if (parts[mini] && parts[mini] <= &part->base[part->n-1]) {
        } else {
            parts[mini] = NULL;
        }

        s_list[i] = min;
    }

    D printf("ID: %d entering merge for loop\n", id);

    pthread_mutex_lock(&init_lock2);
    pass3++;
    while (pass3 != N_THREADS)
        pthread_cond_wait(&cond3, &init_lock2);
    pthread_mutex_unlock(&init_lock2);
    pthread_cond_broadcast(&cond3);

    int s_list_rel = 0;
    for(i=0; i != id; i++)
        s_list_rel += p_size[i];

    memcpy(args->a+s_list_rel, s_list, sizeof(double)*p_size[id]);
    free(s_list);

    D printf("ID: %d done copying to original\n", id);


    return NULL;
#if 0
    pthread_mutex_lock(&init_lock2);
    for (i = 0; i < p_size[id]; i++) {
        printf("%d ",(int)s_list[i]);
    }
    printf("\n\n");

    /*printf("next: %d \n", id);
    for (i = 0; i < N_THREADS; i++){
    	for (j = 0; j < (int)s_partitions[rel+i]->n; j++){
     		printf("%d ", (int)s_partitions[rel+i]->base[j]);
     	}
     	printf("\n\n");
    }*/

    pthread_mutex_unlock(&init_lock2);
#endif
}

void par_sort(
    double*		base,	// Array to sort.
    size_t		n,	// Number of elements in base.
    size_t		s,	// Size of each element.
    int		(*cmp)(const void*, const void*)) // Behaves like strcmp
{

    pthread_t thread[N_THREADS];
    struct thread_args args[N_THREADS];
    int i;
    int rest = n % N_THREADS;
    double* t_base = base;
    for(i = 0; i < N_THREADS; i++) {
        if(rest != 0) {
            args[i].base = t_base;
            args[i].n = n/N_THREADS + 1;
            t_base += n/N_THREADS + 1;
            rest--;
        } else {
            args[i].base = t_base;
            args[i].n = n/N_THREADS;
            t_base += n/N_THREADS;
        }
        args[i].s = sizeof(double);
        args[i].cmp = cmp;
        args[i].id = i;
        args[i].a = base;
    }

    for(i = 0; i != N_THREADS; ++i) {
        pthread_create(&thread[i], NULL, sort, &args[i]);
    }

    for(i = 0; i != N_THREADS; ++i) {
        pthread_join(thread[i], NULL);
    }

}

static int cmp(const void* ap, const void* bp)
{
    double a = *((double*) ap);
    double b = *((double*) bp);
    if (a==b)
        return 0;
    else if (a < b)
        return -1;
    else
        return 1;
}

int main(int ac, char** av)
{
    int		n = SIZE;
    int		i;
    double*		a;
    double	pstart, pend, qstart, qend;

    if (ac > 1)
        sscanf(av[1], "%d", &n);

    srand(getpid());

    a = malloc(n * sizeof a[0]);
    double*	b;
    b = malloc(n * sizeof b[0]);
    for (i = 0; i < n; i++) {
        a[i] = rand();
        b[i] = a[i];
    }

    pstart = sec();
    par_sort(a, n, sizeof a[0], cmp);
    pend = sec();
    qstart = sec();
    qsort(b, n, sizeof b[0], cmp);
    qend = sec();

    printf("Parallel: %1.2f s\n", pend - pstart);
    printf("Single: %1.2f s\n", qend - qstart);
    printf("Speedup: %1.2f\n", ((qend - qstart) / (pend - pstart)));

#ifdef DEBUG
    for (i = 0; i < n; i++) {
        assert(a[i] == b[i]);
    }
#endif

    free(a);
    free(b);

    return 0;
}

