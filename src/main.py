class TestLinter:


    def __init__(self) -> None:
        self.x = 10
        self.emoji = "🐍"

    def about_me(self) -> None:


        "this is a test for the linter"
        x = 5 + 3

        print(f"My value is {self.x}. I am testing the linter {self.emoji}")


def main():
    test = TestLinter()
    test.about_me()


def main2():
  print(    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")


if __name__ == "__main__":
    main()
