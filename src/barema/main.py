from barema.core.generate_report import generate_report
from barema.core.review_data import review_data
from barema.shell import TerminalMenu

if __name__ == "__main__":
    options = [
        ("Vizualizar dados - Desativado", review_data),
        ("Gerar relatório", generate_report),
    ]

    menu = TerminalMenu(options)
    menu.run()
