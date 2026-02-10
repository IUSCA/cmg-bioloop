import api from './api';

class FileSystemService {
  getPathFiles({ path, dirs_only, search_space, extension }) {
    return api.get('/fs', { params: { path, dirs_only, search_space, extension } });
  }
}

export default new FileSystemService();
