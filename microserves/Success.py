from flask import jsonify
def success701():
    return jsonify({"success": "The record has been added"}), 701

def success702():
    return jsonify({"success": "The record has been updated"}), 702

def success703():
    return jsonify({"success": "The record has been deleted"}), 703
