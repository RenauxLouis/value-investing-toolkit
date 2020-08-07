import os
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk

from download_10k import download_and_parse


def askdirectory():
    dirname = filedialog.askdirectory()
    if dirname:
        var.set(dirname)


def UserFileInput(status, name):
    optionFrame = tk.Frame(window)
    optionLabel = tk.Label(window, text=name)#,
                        #    bg="black", fg="white")
    optionLabel.place(relx=0.2, rely=0.4)
    text = status
    var = tk.StringVar(window)
    var.set(text)
    w = tk.Entry(optionFrame, textvariable=var)

    button_folder = ttk.Button(optionFrame,
                               text="Select",
                               command=askdirectory, style="white/black.TLabel")
                                #   bg="black", fg="white")#,
                            #    highlightbackground="black")
    button_folder.pack(side="right")
    w.pack(side="right")
    optionFrame.place(relx=0.3, rely=0.4)
    return w, var


def Print_entry():
    print(var.get())


def download_files():
    download_and_parse([ticker.get()], var.get())
    dl_fullpath = os.path.join(var.get(), ticker.get())
    cap_ticker = ticker.get().upper()
    messagebox.showinfo(
        "", f"{cap_ticker} 10K files downloaded in folder \n{dl_fullpath}")
    os.system(f"open {dl_fullpath}")


def close_window():
    window.destroy()


if __name__ == '__main__':

    # ttk.Style().configure("normal",
    #                 foreground="red",
    #                 background="blue")

    window = tk.Tk()
    s = ttk.Style()
    s.configure('white/black.TLabel', background='black')
    window.title('SEC 10K DOWNLOADER')
    window.geometry("600x400")
    window.config(background="white")

    button_exit = ttk.Button(window,
                             text="Exit",
                             command=close_window, style="white/black.TLabel")
    #   bg="black", fg="white")#,
    #  highlightbackground="black")

    button_exit.place(relx=0.9, rely=0.9, anchor="center")

    w, var = UserFileInput("", "Folder")

    ticker_input = tk.Label(window, text="Ticker")#,
                            # bg="black", fg="white")
    ticker_input.place(relx=0.2, rely=0.3)
    ticker = tk.Entry(window)
    ticker.place(relx=0.3, rely=0.3)

    button_run = ttk.Button(window,
                            text="Download",
                            command=download_files, style="white/black.TLabel")
                            # bg = "black", fg = "white")  # ,
                            # highlightbackground="black")
    button_run.place(relx = 0.38, rely = 0.5)

    window.mainloop()
