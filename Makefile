main: db.c
	gcc -o db.out db.c
	@echo ------
	./db.out

clean:
	rm ./db.out
