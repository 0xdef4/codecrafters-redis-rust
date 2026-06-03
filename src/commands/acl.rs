use crate::acl::sha256_hash;
use crate::protocol::RespValue;
use crate::types::AclDb;

pub fn execute_acl(command: &[String], acl_db: &AclDb) -> Option<RespValue> {
    match command {
        [cmd, sub_cmd] if cmd.to_uppercase() == "ACL" && sub_cmd.to_uppercase() == "WHOAMI" => {
            Some(RespValue::BulkString("default".to_string()))
        }
        [cmd, sub_cmd, username]
            if cmd.to_uppercase() == "ACL" && sub_cmd.to_uppercase() == "GETUSER" =>
        {
            let user = {
                let acl_db = acl_db.lock().unwrap();

                if let Some(user) = acl_db.get(username) {
                    user.clone()
                } else {
                    unimplemented!()
                }
            };

            Some(RespValue::Array(vec![
                RespValue::BulkString("flags".to_string()),
                RespValue::Array(
                    user.get_flags()
                        .iter()
                        .map(|e| RespValue::BulkString(e.to_string()))
                        .collect(),
                ),
                RespValue::BulkString("passwords".to_string()),
                RespValue::Array(
                    user.get_passwords()
                        .iter()
                        .map(|e| RespValue::BulkString(e.to_string()))
                        .collect(),
                ),
            ]))
        }
        [cmd, sub_cmd, username, rest @ ..]
            if cmd.to_uppercase() == "ACL" && sub_cmd.to_uppercase() == "SETUSER" =>
        {
            for ele in rest {
                if let Some(password) = ele.strip_prefix(">") {
                    let hash = sha256_hash(password);

                    {
                        let mut acl_db = acl_db.lock().unwrap();
                        if let Some(user) = acl_db.get_mut(username) {
                            user.store_password(hash.to_lowercase());
                        }
                    }
                }
            }

            Some(RespValue::SimpleString("OK".to_string()))
        }
        _ => unreachable!(),
    }
}

pub fn execute_auth(
    command: &[String],
    acl_db: &AclDb,
    is_authenticated: &mut bool,
) -> Option<RespValue> {
    match command {
        [cmd, username, password] if cmd.to_uppercase() == "AUTH" => {
            let user = {
                let acl_db = acl_db.lock().unwrap();

                if let Some(user) = acl_db.get(username) {
                    user.clone()
                } else {
                    unimplemented!()
                }
            };

            let password_hash_to_validate = sha256_hash(password);

            if user.is_valid_password(password_hash_to_validate) {
                *is_authenticated = true;
                Some(RespValue::SimpleString("OK".to_string()))
            } else {
                Some(RespValue::SimpleError(
                    "WRONGPASS invalid username-password pair or user is disabled.".to_string(),
                ))
            }
        }
        _ => unreachable!(),
    }
}
