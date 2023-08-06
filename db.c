#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

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
  strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
  strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
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
    db_close(table);
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
    return prepare_insert(ib, sm);
  } else if (strncmp(ib->buffer, "select", 6) == 0) {
    sm->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

PrepareResult prepare_insert(InputBuffer* ib, Statement* sm) {
  sm->type = STATEMENT_INSERT;
  char* keyword = strtok(ib->buffer, " ");
  char* id_str = strtok(NULL, " ");
  char* username_str = strtok(NULL, " ");
  char* email_str = strtok(NULL, " ");

  if (id_str == NULL || username_str == NULL || email_str == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_str);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  if (strlen(username_str) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email_str) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  sm->row_to_insert.id = id;
  strcpy(sm->row_to_insert.username, username_str);
  strcpy(sm->row_to_insert.email, email_str);

  return PREPARE_SUCCESS;
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds.\n");
    exit(EXIT_FAILURE);
  }
  if (pager->pages[page_num] == NULL) {
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;
    // 末尾不够一页的补足一页
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }
    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t byte_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (byte_read == -1) {
        printf("Error reading file : %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }
    pager->pages[page_num] = page;
  }

  return pager->pages[page_num];
}

/**
 * @brief 计算table中特定行的内存起始位置
 * @param table 数据表
 * @param row_num 第几行
 * @return 指向该行的内存位置
 */
void* row_slot(Table* table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void* page = get_page(table->pager, page_num);
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
      // printf("This is insert instrument.\n");
      return execute_insert(sm, table);
    case (STATEMENT_SELECT):
      // printf("This is select instrument.\n");
      return execute_select(sm, table);
  }
}

/**
 * @brief 申请一张新Table
 * @return
 */
Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  uint32_t num_rows = pager->file_length / ROW_SIZE;

  Table* table = (Table*)malloc(sizeof(Table));
  table->pager = pager;
  table->num_rows = num_rows;
  return table;
}

Pager* pager_open(const char* filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1) {
    printf("Unable to open file.\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);
  Pager* pager = (Pager*)malloc(sizeof(Pager));

  pager->file_descriptor = fd;
  pager->file_length = file_length;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }
  return pager;
}

void db_close(Table* table) {
  Pager* pager = table->pager;
  uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;
  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }
  uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
  if (num_additional_rows > 0) {
    uint32_t page_num = num_full_pages;
    if (pager->pages[page_num] != NULL) {
      pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
      pager->pages[page_num] = NULL;
    }
  }

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], size);
  if (bytes_written == -1) {
    printf("Error writing:%d\n", errno);
    exit(EXIT_FAILURE);
  }
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
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }
  char* filename = argv[1];
  Table* table = db_open(filename);

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
      case (PREPARE_NEGATIVE_ID):
        printf("ID must be positive.\n");
        continue;
      case (PREPARE_STRING_TOO_LONG):
        printf("String too long.\n");
        continue;
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
