import api from "../api";
import qs from "qs";

function cleanParams(params) {
  return Object.fromEntries(
    Object.entries(params).filter(([_, v]) => v !== null && v !== undefined),
  );
}

class ConversionService {
  getAllDefinitions() {
    return api.get("/conversions/definitions");
  }

  getDefinition(id) {
    return api.get(`/conversions/definitions/${id}`);
  }

  getAll(params = {}) {
    return api.get("/conversions", { params });
  }

  get(
    id,
    params = {
      include_dataset: false,
      include_derived_datasets: false,
      include_definition: false,
    },
  ) {
    return api.get(`/conversions/${id}`, {
      params: cleanParams(params),
      paramsSerializer: (params) =>
        qs.stringify(params, { arrayFormat: "repeat" }),
    });
  }

  create(conversion) {
    return api.post("/conversions", conversion);
  }

  createBulk(data) {
    return api.post("/conversions/bulk", data);
  }

  getDerivedDatasets(id, params = {}) {
    return api.get(`/conversions/${id}/derived_datasets`, { params });
  }

  getLogs(id) {
    return api.get(`/conversions/${id}/logs`);
  }
}

export default new ConversionService();
