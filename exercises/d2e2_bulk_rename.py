import os
import sys
import re
import argparse
import logging

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s'
    )


logger = logging.getLogger(__name__)

# Configure Argument Parser
parser = argparse.ArgumentParser(
    description='Bulk-Rename files in a directory'
    )
parser.add_argument(
    'new_name',
    help=
    """
    Files matching 'file pattern' will be changed to this value.
    An incrementing count will also be added.
    """
    )
parser.add_argument(
    'file_pattern',
    help='Files to rename. (Regex compatible)'
    )
parser.add_argument(
    'target_dir',
    help='Directory of where to rename files inside.'
    )
parser.add_argument(
    '--version', '-v',
    action='version',
    version='%(prog)s 1.0.0'
    )

args = parser.parse_args()


# Main Method
def main():
    try:
        files_to_rename = [
            f for f in os.listdir(args.target_dir)
            if re.search(args.file_pattern, f)
            ]
        count = 0
    except (FileNotFoundError, FileExistsError) as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    for file in files_to_rename:
        count += 1
        filename, file_extension = os.path.splitext(file)
        logger.debug(f"File: {file}")
        new_filename = args.new_name + str(count) + file_extension
        try:
            os.rename(
                os.path.join(args.target_dir, file),
                os.path.join(args.target_dir, new_filename)
            )
            logger.info(f"Renamed {file} to {new_filename}")
        except IsADirectoryError as e:
            logger.error(f"IsADirectoryError: {e}")
            sys.exit(1)
        except NotADirectoryError as e:
            logger.error(f"NotADirectoryError: {e}")
            sys.exit(1)
        except PermissionError as e:
            logger.error(f"PermissionError: {e}")
            sys.exit(1)
        except OSError as e:
            logger.error(f"OSError: {e}")
            sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
