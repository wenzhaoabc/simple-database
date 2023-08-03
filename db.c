#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

typedef struct {
  char* buffer;
  size_t buffer_length;
  size_t input_length;
} InputBuffer;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATEMENT,
} PrepareResult;

/**
 * @brief 指令类型
 */
typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
} StatementType;

typedef struct {
  StatementType type;
} Statement;

/**
 * @brief 执行元指令
 * @param ib 输入字符串
 * @return 执行结果
 */
MetaCommandResult do_meta_command(InputBuffer* ib) {
  if (strcmp(ib->buffer, ".exit") == 0) {
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
    return PREPARE_SUCCESS;
  } else if (strncmp(ib->buffer, "select", 6) == 0) {
    sm->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* sm) {
  switch (sm->type) {
    case (STATEMENT_INSERT):
      printf("This is insert instrument.\n");
      break;
    case (STATEMENT_SELECT):
      printf("This is select instrument.\n");
      break;
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
  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer)) {
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
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of %s \n", input_buffer->buffer);
        continue;
    }

    execute_statement(&statement);
    printf("Executed.\n");
  }
  return 0;
}
