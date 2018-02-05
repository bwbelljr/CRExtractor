import os
import sqlite3
from pattern.en import parsetree
from pattern.search import search

def decode_utf8(string):
    if isinstance(string, str):
        for encoding in (('utf-8',), ('windows-1252',), ('utf-8', 'ignore')):
            try:
                return string.decode(*encoding)
            except:
                pass
        return string # Don't know how to handle it...
    return unicode(string, 'utf-8')

def insert_causal_relation_into_db(cause, effect, source):
    # Simple function to insert causal relation into database
    with conn:
        c.execute("INSERT INTO cause_effect VALUES (:cause, :effect, :source)",
            {'cause':cause, 'effect':effect, 'source':source})

def get_causal_relations_by_effect(effect):
    # Simple function to return all database records with some effect
    c.execute("SELECT * FROM cause_effect WHERE effect=:effect",
                {'effect': effect})
    return c.fetchall()

def get_causal_relations_by_cause(cause):
    # Simple function to return all database records with some cause
    c.execute("SELECT * FROM cause_effect WHERE cause=:cause",
                {'cause': cause})
    return c.fetchall()

def remove_causal_relation_from_db(cause, effect, source):
    # Simple function to remove a database record with specific cause,
    # effect, and source
    with conn:
        c.execute("""DELETE from cause_effect WHERE cause=:cause AND
                effect=:effect AND source=:source""",
                    {'cause':cause, 'effect':effect, 'source':source})

def extract_chunk_match(sentence):
    # This function checks for and returns relations in sentence
    # check if sentence has relation "X cause* Y"
    if str(search('{NP} cause {NP}', sentence)) != '[]':
        chunk_match = search('{NP} cause {NP}', sentence)
        relation_form = "X causes Y"
    # check if sentence has relation "X is caused by Y"
    elif str(search('{NP} is caused by {NP}', sentence)) != '[]':
        chunk_match = search('{NP} is caused by {NP}', sentence)
        relation_form = "X caused by Y"
    # Check is sentence has relation "X are caused by Y"
    elif str(search('{NP} are caused by {NP}', sentence)) != '[]':
        chunk_match = search('{NP} are caused by {NP}', sentence)
        relation_form = "X caused by Y"
    # Any other result is no match
    else:
        chunk_match = ""
        relation_form = "none"
    # Create and return list with chunk_match and relation_form
    return [chunk_match, relation_form]

def chunk_to_string(chunk):
    # This function converts chunk string from Pattern into Python string
    # turn chunk into string object

    # chunk_string = str(chunk)
    chunk_string = chunk.string

    # remove leading characters: Chunk('
    # This amounts to starting the string at the 7th character, as "Chunk('"
    # represents the first 6 characters
    chunk_string = chunk_string[7:]
    # Remove the ending of the chunk string, where we get "/" followed
    # by POS tag. We do this by finding the position of '/' and subsetting
    # our string up until this point
    chunk_string = chunk_string[:chunk_string.find('/')]
    return chunk_string

def relation_to_NP(sentence_relation):
    # This function transforms extracted sentence relations
    # into noun phrases representing cause and effect
    # We return a list with [cause noun phrase, effect noun phrase]
    if sentence_relation[1] == "X causes Y":
        for match in sentence_relation[0]:
            cause_NP = chunk_to_string(match.constituents()[0])
            effect_NP = chunk_to_string(match.constituents()[-1])
    elif sentence_relation[1] == "X caused by Y":
        for match in sentence_relation[0]:
            cause_NP = chunk_to_string(match.constituents()[-1])
            effect_NP = chunk_to_string(match.constituents()[0])
    else:
        cause_NP = ''
        effect_NP = ''
    return [cause_NP, effect_NP]

def check_insert_relations_into_db(current_article, source):
    current_article = parsetree(current_article, relations=True, lemmata=True, encoding='utf-8')
    for sentence in current_article:
        chunk_match = extract_chunk_match(sentence)
        if chunk_match[1] != "none":
            causal_relation = relation_to_NP(chunk_match)
            cause = causal_relation[0]
            effect = causal_relation[1]
            insert_causal_relation_into_db(cause, effect, source)

def read_and_extract_articles(strText):
    # Determine end of file index
    end_of_file_index = strText.find("GSPLIT:u WestburyLab.Wikipedia.Corpus.txt")-1

    # initialize the beginning of the document such that the first article
    # follows the first "END.OF.DOCUMENT"
    # We will potentially skip 6 articles, which can be fixed later
    article_start_index = 1
    article_end_index = strText.find("---END.OF.DOCUMENT---", article_start_index)
    article_start_index = article_end_index+23
    article_end_index = strText.find("---END.OF.DOCUMENT---", article_start_index)

    while (article_start_index < end_of_file_index and article_end_index != -1):
        end_of_title_index = strText.find(".\n", article_start_index)
        article_title = strText[article_start_index:end_of_title_index]
        source = article_title
        current_article = strText[article_start_index:article_end_index]

        current_article = decode_utf8(current_article)

        # HERE IS WHERE WE PARSE AND EXTRACT RELATIONS
        check_insert_relations_into_db(current_article, source)

        # Find indices for start and end of next article
        article_start_index = article_end_index+23
        article_end_index = strText.find("---END.OF.DOCUMENT---", article_start_index)

def read_and_extract_wikipedia_file(file_location):
    # Open Wikipedia file
    f = open(file_location)

    # Read entire file contents
    strText = f.read()

    # Pass contents of file to read_and_extract_articles function
    read_and_extract_articles(strText)

    # Status message to show that we reached end of document
    print "we reached the end of the document", file_location

    # Close the file
    f.close()

###############################################################################

# We create a file-based database
conn = sqlite3.connect("causes.db")
conn.text_factory = str

c= conn.cursor()

# Create the cause_effect table
#c.execute("""CREATE TABLE cause_effect(
#            cause text,
#            effect text,
#            source text
#            )""")

# Commit creation of cause_effect table
#conn.commit()

# Find current directory
dir_path = os.path.dirname(os.path.realpath(__file__))

# Create file location based on current directory and wikipedia files
for i in range(1,7):
    file_location = dir_path+"\WestburyLab.Wikipedia.Corpus.txt.bz2\wikipedia_big"+str(i)+".txt"
    read_and_extract_wikipedia_file(file_location)

# Close the database connection
conn.close()
