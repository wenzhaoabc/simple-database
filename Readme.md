# A Simple Datebase

## 简介

参照[db_tutorial](https://github.com/cstack/db_tutorial)编写的简易数据库，所有代码在[db.c](./db.c)文件中，数据库整体结构仿照sqlite，单线程运行，支持索引，事务处理。

## Part 1

完成REPL(read - eval - print - loop)

## Part 2

完成项目前端骨架，接收用户输入，判断是否属于元指令(即非数据操作指令)，解析元指令并执行，如果为数据操作指令，即SQL语句，解析SQL语句，构造要执行的指令描述(`Statement`)，执行该`Statement`

## Part 3

An In-Memory, Append-Only, Single-Table Database

内存中单表数据库，命令解析，插入，执行
