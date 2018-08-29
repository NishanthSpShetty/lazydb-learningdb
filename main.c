#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)-> Attribute)

#define TABLE_MAX_PAGES 100
#define COLUMN_USERNAME_SIZE  32
#define COLUMN_EMAIL_SIZE 255
/**
 *
 * file main.c
 * Author   : Nishanth Shetty <nishanthspshetty@gmail.com>
 * Date     : 26-08-2018
 **/


////BEGIN DEFINE_TYPE :DEFINE ALL TYPES HERE

//input buffer structure 
struct InputBuffer_t {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
};

typedef struct InputBuffer_t InputBuffer;


enum MetaCommandResult_t{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

typedef enum MetaCommandResult_t MetaCommandResult;

enum PrepareResult_t {
    PREPARE_SYNTAX_ERROR,
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
};

typedef enum PrepareResult_t PrepareResult;

//statement types
enum StatementType_t {
    STATEMENT_INSERT,
    STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;



struct Row_t {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
};

typedef struct Row_t Row;

//struct defining sql statement
struct Statement_t {
    StatementType type;
    Row row_to_insert;
};
typedef struct Statement_t Statement;


////define table metadata for test table-(hard caded table structure
//
const uint32_t ID_SIZE = size_of_attribute(Row,id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET+ ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET+USERNAME_SIZE;
const uint32_t ROWS_SIZE = ID_SIZE + USERNAME_SIZE +EMAIL_SIZE;

//allocate page size of 4K, os allocates in block of 4k so, be safe from page swap breaking data
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE  = PAGE_SIZE / ROWS_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE + TABLE_MAX_PAGES;

struct Table_t {
    void *pages[TABLE_MAX_PAGES];
    uint32_t num_rows;
};
typedef struct Table_t Table;


enum ExecuteResult_t { 
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS
};

typedef enum ExecuteResult_t ExecuteResult;
////END DEFINE_TYPE


///// util funcs 
//serialize and deserialize page
void serialize_row(Row * source , void * destination){
    memcpy(destination+ID_OFFSET , &(source->id), ID_SIZE);
    memcpy(destination+USERNAME_OFFSET, &(source -> username),USERNAME_SIZE) ;
    memcpy(destination +EMAIL_OFFSET, &(source -> email), EMAIL_SIZE);
}

void deserialize_row(void *source , Row * destination){
    memcpy(&(destination->id), source+ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source+USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source+EMAIL_OFFSET , EMAIL_SIZE );
}

//get row_slot -> to where we do rw op
void * row_slot(Table * table, uint32_t row_num){
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void * page = table -> pages[page_num]; // get the page from the table
    if (!page){ //if null 
        //allocate new page and store it to table
        page =  table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROWS_SIZE;

    return page + byte_offset;
}



//instantiate new buffer
InputBuffer *new_input_buffer(){
    InputBuffer * input_buffer = malloc(sizeof(InputBuffer));
    input_buffer -> buffer =NULL;
    input_buffer -> buffer_length = 0;
    input_buffer -> input_length = 0;
    return input_buffer;
}

//read input and write input_buffer
void read_input(InputBuffer * input_buffer){
        //printf("reading input\n");
        ssize_t bytes_read = getline(&(input_buffer -> buffer), &(input_buffer -> input_length), stdin);
        if(bytes_read<=0){
                printf("Error reading input.\n");
                exit(EXIT_FAILURE);
        }
        //ignore trailing new line
        input_buffer->input_length = bytes_read - 1;
        input_buffer->buffer[bytes_read-1] = 0; //NULL terminator
        //printf(" inpur read  %s \n ",input_buffer-> buffer);
}


void print_prompt() {
        printf("lazydb > ");
}

//printer
void print_row(Row *row){
    printf("(%d, %s, %s)\n", row->id , row->username, row->email);
}

//do_meta_command 

MetaCommandResult do_meta_command(InputBuffer * input_buffer){
    if(strcmp(input_buffer -> buffer, ".exit") == 0){
        exit(EXIT_SUCCESS);
    }else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


//prepare sql statement from input buffer string
PrepareResult prepare_statement(InputBuffer * input_buffer, Statement * statement){
    if(strncmp(input_buffer-> buffer, "insert", 6) == 0) {
        //starts with insert
        statement->type = STATEMENT_INSERT;
        //parse the isert statement

        int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s",  &statement->row_to_insert.id, &statement->row_to_insert.username, & statement-> row_to_insert.email);
        //printf("prepared statement\n"); 
        if(args_assigned < 3){
            return PREPARE_SYNTAX_ERROR;
        }

        return PREPARE_SUCCESS;

    }
    if(strncmp(input_buffer->buffer, "select",6) == 0){
        statement-> type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    //unable to figure wth user entered
    return PREPARE_UNRECOGNIZED_STATEMENT;
}


//execute insert query statement
ExecuteResult execute_insert(Statement *statement,Table *table){

    //check if the table is full
    if(table -> num_rows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }

    Row * row_to_insert = &(statement->row_to_insert);
    //printf("row to insert %d %s %s ", row_to_insert->id, row_to_insert->username, row_to_insert->email); 
    //write to table row
    serialize_row(row_to_insert, row_slot(table,table->num_rows));
    //inc row count
    table->num_rows++;
    return EXECUTE_SUCCESS;
}



//execute select statement
ExecuteResult execute_select(Statement *statement, Table * table){
    Row row;
    for(uint32_t i = 0; i< table -> num_rows ; i++) {
            deserialize_row(row_slot(table, i), &row);
            print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

//execute statement -> calls appropriate query executor
ExecuteResult execute_statement(Statement *statement, Table * table){
    switch(statement -> type){
        case STATEMENT_INSERT:
            //printf("im gonna insert something here");
            return execute_insert(statement,table);
            //break;
        case STATEMENT_SELECT:
            //printf("this is where im gonna select data from ");
            //break;
            return execute_select(statement, table);
    }
}


//forgot table initializer
Table * new_table(){
        Table * table = malloc(sizeof(Table));
        table->num_rows = 0;
        return table;
}

/**
 * main
 * the world start to begin here 
 */
void main(int argc, char *argv[]){
        InputBuffer * input_buffer = new_input_buffer();
        Table *table = new_table();
        //loop infinetely, for repl
        
        while(true){
            print_prompt();
            read_input(input_buffer);
            
            
            if( input_buffer->buffer[0] == '.' ){
                switch(do_meta_command(input_buffer)){
                    case META_COMMAND_SUCCESS :
                        continue;
                    case META_COMMAND_UNRECOGNIZED_COMMAND:
                       // printf("Unrecognized command '%s'.\n", input_buffer-> buffer);
                        continue;
                }

            }

           // printf("processing statement ... ");
            Statement statement;
            switch(prepare_statement (input_buffer, &statement)){
                case PREPARE_SUCCESS:
                        break;
                case PREPARE_SYNTAX_ERROR:
                        printf("Syntax error, could not parse statement.\n");
                        continue;

                case PREPARE_UNRECOGNIZED_STATEMENT:
                        printf("Unrecognized keyword at the start of '%s'.\n", input_buffer->buffer);
                        continue;
            }

            
            switch(execute_statement(&statement,table)){
                case EXECUTE_SUCCESS:
                    printf("Executed.\n");
                    break;
                case EXECUTE_TABLE_FULL:
                    printf("Error : Table full");
                    break;
            }

        }
}
