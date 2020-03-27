import re

substring = "you"

pattern = re.compile("y\w+")

string = """I don't think that you even realize
The joy you make me feel when I'm inside
Your universe
You hold me like I'm the one who's precious
I hate to break it to you but it's just
The other way around
You can thank your stars all you want but
I'll always be the lucky one"""

for word in string.split():
    print(re.match(substring, word))

for word in string.split():
    print(pattern.match(string))
    
print("\nSearch")

print(re.search(substring, string))
print(pattern.search(string))

print(pattern.findall(string))
    
print("\nList Match")

shows = ["Stranger Things", "The Crown", "Sabrina", 
"The Witcher", "Orange is the New Black", 
"Black Mirror", "The Umbrella Academy"]

for show in shows:
    match = re.match("(T\w+)\W", show)
    print(match)
