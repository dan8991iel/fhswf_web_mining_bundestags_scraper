



def extract_bio(minister):

    bio = []
    started = False
    stopped = False

    for item in minister["wiki_content"]:
        content_type = item.get("type")
        if content_type == "p": started = True
