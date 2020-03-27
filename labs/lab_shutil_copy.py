import os
import shutil

path = '/home/ubuntu/environment/projects/kungfu/'

source = path + 'source/'

sourcefile = source + 'sample.txt'

destination = path + 'destination/'

print('Before Copying:')
print("Source:", os.listdir(source))
print("Destination:", os.listdir(destination))

dest = shutil.copy(sourcefile, destination)

print("After Copying:")
print("Source:", os.listdir(source))
print("Destination:", os.listdir(destination))

print("Destination Path:", dest)
