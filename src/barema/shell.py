import os


class TerminalMenu:
    def __init__(self, items):
        self.items = items

    def clear_screen(self):
        os.system("cls" if os.name == "nt" else "clear")

    def run(self):
        while True:
            self.clear_screen()
            print("=" * 30)
            print("        MENU PRINCIPAL")
            print("=" * 30)

            for index, (label, _) in enumerate(self.items, 1):
                print(f"[{index}] {label}")

            exit_option = len(self.items) + 1
            print(f"[{exit_option}] Sair")
            print("-" * 30)

            try:
                choice = int(input("Escolha uma opção: "))

                if choice == exit_option:
                    break

                if 1 <= choice <= len(self.items):
                    self.clear_screen()
                    self.items[choice - 1][1]()
                    input("\nPressione Enter para voltar...")
                else:
                    input("\nOpção incorreta. Pressione Enter...")
            except ValueError:
                input("\nEntrada inválida. Pressione Enter...")
