from tkinter import *
from tkinter import filedialog


def askdirectory():
    dirname = filedialog.askdirectory()
    if dirname:
        var.set(dirname)


def UserFileInput(status, name):
    optionFrame = Frame(window)
    text = status
    var = StringVar(window)
    var.set(text)
    w = Entry(optionFrame, textvariable=var)

    button_folder = Button(optionFrame,
                           text="Folder",
                           command=askdirectory,
                           bg="black",
                           highlightbackground="black")
    button_folder.pack(side="left")
    w.pack(side="right")
    optionFrame.place(relx=0.3, rely=0.3)
    return w, var


def Print_entry():
    print(var.get())


if __name__ == '__main__':
    window = Tk()
    window.title('SEC 10K DOWNLOADER')
    window.geometry("600x400")
    window.config(background="black")

    button_exit = Button(window,
                         text="Exit",
                         command=exit,
                         bg="black",
                         highlightbackground="black")

    button_exit.place(relx=0.9, rely=0.9, anchor="center")

    w, var = UserFileInput("", "")

    window.mainloop()
