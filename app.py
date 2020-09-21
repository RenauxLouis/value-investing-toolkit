from flask import Flask
from commands import usersbp

app = Flask(__name__)
# you MUST register the blueprint
app.register_blueprint(usersbp)
