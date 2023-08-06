import subprocess
import os


def test_case(input: str, expect: str):
    exe = subprocess.Popen(
        ["./db.out", "mydb.db"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        # bufsize=1,
        universal_newlines=True,
    )
    input = input + "\n.exit\n"
    expect = f"db > {expect}\ndb > "
    stdout, stderr = exe.communicate(input=input, timeout=10)
    if stdout == expect:
        print(f"Test case {input.split(' ')[0]} pass!")
    else:
        print("Test case error.\n")
        print(f"input = {input}")
        print(f"error = {stderr}")


if not os.path.exists("db.out"):
    _ = subprocess.run(
        ["gcc", "-o", "db.out", "db.c"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
else:
    print("Targaet file has been compiled.")

test_case("insert -1 wen zhao@wen.com", "ID must be positive.")
test_case("insert 10 {0} {1}".format("name" * 10, "email" * 10), "String too long.")
test_case("insert 10 asd asd.com", "Executed.")
test_case("insert 1 asd", "Syntax error. Canot parse statement.")
test_case(
    "select",
    "1 wen zhao@wen.com\n"
    + "2 wen zhao@wen.com\n"
    + "3 wen zhao@wen.com\n"
    + "4 wen zhao@wen.com\n"
    + "Executed.\n",
)
