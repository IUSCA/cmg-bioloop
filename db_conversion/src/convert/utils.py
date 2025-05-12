def extract_directories(file_path):
  parts = file_path.split('/')
  directories = parts[:-1]  # All parts except the last one (which is the file name)
  return directories


def get_parent_path(path):
  parts = path.split('/')
  if len(parts) > 1:
    parts.pop()  # Remove the last part (file or directory name)
    return ''.join(parts)
  return ''  # Return empty string if there's no parent (top-level file)


def mongo_file_to_pg_file(file: dict) -> dict:
  return {
    "name": file.get("path", "").split("/")[-1],
    "path": file.get("path"),
    "md5": file.get("md5"),
    "size": file.get("size"),
  }


__all__ = ['extract_directories', 'get_parent_path', 'mongo_file_to_pg_file']
