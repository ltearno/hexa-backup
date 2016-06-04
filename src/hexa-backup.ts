import fs = require('fs');

console.log("Welcome to Hexa-Backup !");

/*

Objects :
NAME => CONTENT

ObjectsRepository
    getObject(String name)
    putObject(byte[] content) : String name

ReferenceRepository
    getObjectByReference(String reference): String objectName
    putObjectReference(String reference, String objectName) : String previousObjectName

References :
HEAD => nom de l'object contenant la description de HEAD
WATCHED_DIRECTORY =>
*/

/*
Structures de données:

COMMIT :
    id_commit_precedent
    sha correspondant au contenu de la sauvegarde
    meta data : auteur, date de commit, machine sur laquelle le commit a ete fait, etc...

REPERTOIRE :
    liste de
        nom du fichier
        attributs
        content object name
*/