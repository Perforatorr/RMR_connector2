from flask import jsonify
def error601():
    return jsonify({"error": "The code of equipment is missing or incorrect"}), 601

def error602():
    return jsonify({"error": "The start timestamp is missing or incorrect"}), 602

def error603():
    return jsonify({"error": "The end timestamp is missing or incorrect"}), 603

def error604():
    return jsonify({"error": "The equipment condition is missing or incorrect"}), 604

def error605():
    return jsonify({"error": "The document number is missing or incorrect"}), 605

def error606():
    return jsonify({"error": "The equipment with the required code was not found"}), 606

def error607():
    return jsonify({"error": "The unknown document number"}), 607

def error608():
    jsonify({"error":"The unknown equipment condition"}),608

def error609():
    return jsonify({"error": "The method is missing or incorrect"}), 609

def error610():
    return jsonify({"error": "The time limit for modifying the record has expired"}), 610

def error611():
    return jsonify({"error":"Invalid authentication"}),611
