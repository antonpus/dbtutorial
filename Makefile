SRC = db.c query/query.c storage/storage.c
OUTPUT = db.out
DBFILE = mydb.db

db: $(SRC)
	clang $(SRC) -o $(OUTPUT)
run: db
	./$(OUTPUT) $(DBFILE)