def fizz_buzz(n):
    for i in range(1, n):
        if i%3 == 0:
            print("fizz")
        elif i%5 == 0:
            print("buzz")
        else:
            print(i)


fizz_buzz(10)
fizz_buzz(15)
fizz_buzz(33)