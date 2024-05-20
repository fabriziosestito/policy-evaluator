use anyhow::{anyhow, Result};
use std::ops::Deref;

#[derive(Debug, PartialEq)]
pub(crate) enum Operator {
    Equals,
    NotEquals,
}

type Requirement = (String, String, Operator);

#[derive(Debug, PartialEq)]
pub(crate) struct Selector {
    requirements: Vec<Requirement>,
}

impl Selector {
    pub(crate) fn from_string(input: &str) -> Result<Self> {
        let mut selector = Selector {
            requirements: Vec::new(),
        };

        if input.is_empty() {
            return Ok(selector);
        }

        let pairs: Vec<&str> = input.split(',').collect();

        for pair_str in pairs {
            let requirement = if pair_str.contains("!=") {
                let mut pair = pair_str.split("!=");
                (
                    pair.next()
                        .ok_or(anyhow!("Invalid key-value pair"))?
                        .to_owned(),
                    pair.next()
                        .ok_or(anyhow!("Invalid key-value pair"))?
                        .to_owned(),
                    Operator::NotEquals,
                )
            } else if pair_str.contains("==") {
                let mut pair = pair_str.split("==");
                (
                    pair.next()
                        .ok_or(anyhow!("Invalid key-value pair"))?
                        .to_owned(),
                    pair.next()
                        .ok_or(anyhow!("Invalid key-value pair"))?
                        .to_owned(),
                    Operator::Equals,
                )
            } else if pair_str.contains('=') {
                let mut pair = pair_str.split('=');
                (
                    pair.next()
                        .ok_or(anyhow!("Invalid key-value pair"))?
                        .to_owned(),
                    pair.next()
                        .ok_or(anyhow!("Invalid key-value pair"))?
                        .to_owned(),
                    Operator::Equals,
                )
            } else {
                return Err(anyhow!("Invalid operator"));
            };

            selector.requirements.push(requirement);
        }

        Ok(selector)
    }
}

impl Deref for Selector {
    type Target = Vec<Requirement>;

    fn deref(&self) -> &Self::Target {
        &self.requirements
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_string_valid_input() {
        let input = "key1=value1,key2==value2,key3!=value3";
        let expected_selector = Selector {
            requirements: vec![
                ("key1".to_string(), "value1".to_string(), Operator::Equals),
                ("key2".to_string(), "value2".to_string(), Operator::Equals),
                (
                    "key3".to_string(),
                    "value3".to_string(),
                    Operator::NotEquals,
                ),
            ],
        };

        assert_eq!(Selector::from_string(input).unwrap(), expected_selector);
    }

    #[test]
    fn test_from_string_invalid_operator() {
        let input = "key1=value1,key2<value2";

        assert!(Selector::from_string(input).is_err());
    }

    #[test]
    fn test_from_string_invalid_key_value_pair() {
        let input = "key1=value1,key2";

        assert!(Selector::from_string(input).is_err());
    }

    #[test]
    fn test_from_string_empty_input() {
        let input = "";
        let expected_selector = Selector {
            requirements: Vec::new(),
        };

        assert_eq!(Selector::from_string(input).unwrap(), expected_selector);
    }

    #[test]
    fn test_iterator() {
        let input = "key1=value1,key2==value2,key3!=value3";
        let selector = Selector::from_string(input).unwrap();
        let mut iter = selector.iter();

        assert_eq!(
            iter.next(),
            Some(&("key1".to_string(), "value1".to_string(), Operator::Equals))
        );

        assert_eq!(
            iter.next(),
            Some(&("key2".to_string(), "value2".to_string(), Operator::Equals))
        );

        assert_eq!(
            iter.next(),
            Some(&(
                "key3".to_string(),
                "value3".to_string(),
                Operator::NotEquals
            ))
        );

        assert_eq!(iter.next(), None);
    }
}
