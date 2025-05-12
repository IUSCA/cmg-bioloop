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


__all__ = ['extract_directories', 'get_parent_path']
