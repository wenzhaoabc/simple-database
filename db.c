#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "db.h"

/**
 * @brief 输出Row
 * @param row 指向该行的指针
 */
void print_row(Row* row) {
  printf("%d %s %s\n", row->id, row->username, row->email);
}

/**
 * @brief 将Row结构体存入指定位置
 * @param source Row结构体
 * @param destination 目标位置
 */
void serialize_row(Row* source, void* destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

/**
 * @brief 从存储位置构造Row结构体
 * @param source 原始存储位置
 * @param destination 目标结构体
 */
void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

/**
 * @brief 执行元指令
 * @param ib 输入字符串
 * @return 执行结果
 */
MetaCommandResult do_meta_command(InputBuffer* ib, Table* table) {
  if (strcmp(ib->buffer, ".exit") == 0) {
    close_input_buffer(ib);
    free_table(table);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

/**
 * @brief 编译SQL指令
 * @param ib 输入指令
 * @param sm 指令类型
 * @return 编译成功与否
 */
PrepareResult prepare_statement(InputBuffer* ib, Statement* sm) {
  if (strncmp(ib->buffer, "insert", 6) == 0) {
    sm->type = STATEMENT_INSERT;
    int args_assigned =
        sscanf(ib->buffer, "insert %d %s %s", &(sm->row_to_insert.id),
               sm->row_to_insert.username, sm->row_to_insert.email);
    if (args_assigned < 3) {
      return PREPARE_SYNTAX_ERROR;
    }
    return PREPARE_SUCCESS;
  } else if (strncmp(ib->buffer, "select", 6) == 0) {
    sm->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

/**
 * @brief 计算table中特定行的内存起始位置
 * @param table 数据表
 * @param row_num 第几行
 * @return 指向该行的内存位置
 */
void* row_slot(Table* table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void* page = table->pages[page_num];
  if (page == NULL) {
    page = table->pages[page_num] = malloc(PAGE_SIZE);
  }
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t bytes_offset = row_offset * ROW_SIZE;

  return page + bytes_offset;
}

/**
 * @brief 执行插入指令
 * @param sm 指令信息
 * @param table 要插入的表格
 * @return 执行成功与否
 */
ExecuteResult execute_insert(Statement* sm, Table* table) {
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(sm->row_to_insert);
  serialize_row(row_to_insert, row_slot(table, table->num_rows));
  table->num_rows += 1;

  return EXECUTE_SUCCESS;
}

/**
 * @brief 执行查询指令
 * @param sm 指令信息
 * @param table 要查询的表格
 * @return 执行成功与否
 */
ExecuteResult execute_select(Statement* sm, Table* table) {
  Row row;
  for (uint32_t i = 0; i < table->num_rows; i++) {
    deserialize_row(row_slot(table, i), &row);
    print_row(&row);
  }
  return EXECUTE_SUCCESS;
}

/**
 * @brief 指令执行
 * @param sm
 */
ExecuteResult execute_statement(Statement* sm, Table* table) {
  switch (sm->type) {
    case (STATEMENT_INSERT):
      printf("This is insert instrument.\n");
      return execute_insert(sm, table);
    case (STATEMENT_SELECT):
      printf("This is select instrument.\n");
      return execute_select(sm, table);
  }
}

/**
 * @brief 申请一张新Table
 * @return
 */
Table* new_table() {
  Table* table = (Table*)malloc(sizeof(Table));
  table->num_rows = 0;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    table->pages[i] = NULL;
  }
  return table;
}

/**
 * @brief 释放表空间
 * @param table
 */
void free_table(Table* table) {
  for (uint32_t i = 0; table->pages[i]; i++) {
    free(table->pages[i]);
  }
  free(table);
}

/**
 * @brief 获取输入
 * @return 输入的数据
 */
InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = (InputBuffer*)calloc(1, sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

/**
 * @brief 输出提示语
 */
void print_prompt() { printf("db > "); }

/**
 * @brief 获取用户输入指令
 * @param ib 输入数据的缓冲区
 */
void read_input(InputBuffer* ib) {
  ssize_t bytes_read = getline(&(ib->buffer), &(ib->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }
  ib->input_length = bytes_read - 1;
  ib->buffer[bytes_read - 1] = '\0';
}

/**
 * @brief 释放暂存输入数据的缓冲区
 * @param ib 暂存输入数据的缓冲区
 */
void close_input_buffer(InputBuffer* ib) {
  free(ib->buffer);
  free(ib);
}

int main(int argc, char* argv[]) {
  Table* table = new_table();

  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
        case (META_COMMAND_SUCCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command '%s' \n", input_buffer->buffer);
          continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case (PREPARE_SYNTAX_ERROR):
        printf("Syntax error. Canot parse statement.\n");
        continue;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of %s \n", input_buffer->buffer);
        continue;
    }

    switch (execute_statement(&statement, table)) {
      case (EXECUTE_SUCCESS):
        printf("Executed.\n");
        break;
      case (EXECUTE_TABLE_FULL):
        printf("Error: table full.\n");
        break;
    }
  }
  return 0;
}
