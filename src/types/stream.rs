use crate::protocol::RespValue;

#[derive(Debug, Clone)]
pub struct StreamEntry {
    id: String,
    fields: Vec<(String, String)>,
}

impl StreamEntry {
    pub fn new(id: String, fields: Vec<(String, String)>) -> Self {
        Self { id, fields }
    }

    pub fn get_entry_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_fields(&self) -> Vec<(String, String)> {
        self.fields.clone()
    }

    pub fn to_resp_value(&self) -> RespValue {
        let mut output = Vec::new();
        output.push(RespValue::BulkString(self.get_entry_id()));
        let mut fields_vec = Vec::new();
        for e in self.get_fields() {
            fields_vec.push(RespValue::BulkString(e.0));
            fields_vec.push(RespValue::BulkString(e.1));
        }
        output.push(RespValue::Array(fields_vec));

        RespValue::Array(output)
    }
}
