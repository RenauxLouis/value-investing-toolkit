from tkinter import *
from tkinter import filedialog
from tkinter import messagebox

from download_10k import download


def askdirectory():
    dirname = filedialog.askdirectory()
    if dirname:
        var.set(dirname)


def UserFileInput(status, name):
    optionFrame = Frame(window)
    optionLabel = Label(window, text=name,
                        bg="black", fg="white")
    optionLabel.place(relx=0.2, rely=0.4)
    text = status
    var = StringVar(window)
    var.set(text)
    w = Entry(optionFrame, textvariable=var)

    button_folder = Button(optionFrame,
                           text="Select",
                           command=askdirectory,
                           highlightbackground="black")
    button_folder.pack(side="right")
    w.pack(side="right")
    optionFrame.place(relx=0.3, rely=0.4)
    return w, var


def Print_entry():
    print(var.get())


def download_files():
    download([ticker.get()], var.get())
    dl_fullpath = os.path.join(var.get(), ticker.get())
    messagebox.showinfo(
        "", f"{ticker.get()} 10K files downloaded in folder {dl_fullpath}")


if __name__ == '__main__':
    window = Tk()
    window.title('SEC 10K DOWNLOADER')
    window.geometry("600x400")
    window.config(background="black")

    button_exit = Button(window,
                         text="Exit",
                         command=exit,
                         highlightbackground="black")

    button_exit.place(relx=0.9, rely=0.9, anchor="center")

    w, var = UserFileInput("", "Folder")

    ticker_input = Label(window, text="Ticker",
                         bg="black", fg="white")
    ticker_input.place(relx=0.2, rely=0.3)
    ticker = Entry(window)
    ticker.place(relx=0.3, rely=0.3)

    button_run = Button(window,
                        text="Download",
                        command=download_files,
                        bg="black",
                        highlightbackground="black")
    button_run.place(relx=0.38, rely=0.5)

    window.mainloop()
