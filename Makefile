main: db.c
	gcc -o db db.c
	@echo ------
	./db

clean:
	rm ./db
