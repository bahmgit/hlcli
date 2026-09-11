use std::{fmt, str::FromStr};

use alloy_dyn_abi::Eip712Domain;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_signer::SignerSync;
use alloy_sol_types::{eip712_domain, sol};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Chain {
    Mainnet,
    Testnet,
}

impl Chain {
    pub fn is_mainnet(self) -> bool {
        self == Self::Mainnet
    }

    pub fn source(self) -> &'static str {
        if self.is_mainnet() { "a" } else { "b" }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    Perp,
    BuilderPerp,
    Spot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetId(pub u32);

impl AssetId {
    pub fn native_perp(index: u32) -> Self {
        Self(index)
    }

    pub fn builder_perp(dex_index: u32, index: u32) -> Self {
        Self(100_000 + dex_index * 10_000 + index)
    }

    pub fn spot(spot_index: u32) -> Self {
        Self(10_000 + spot_index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceTick {
    max_decimals: i64,
}

impl PriceTick {
    pub fn new(kind: MarketKind, size_decimals: i64) -> Self {
        let base = match kind {
            MarketKind::Perp | MarketKind::BuilderPerp => 6,
            MarketKind::Spot => 8,
        };
        Self {
            max_decimals: base - size_decimals,
        }
    }

    fn decimal_places(self, price: Decimal) -> Option<u32> {
        if price <= Decimal::ZERO {
            return None;
        }
        let max_decimals = u32::try_from(self.max_decimals)
            .ok()
            .filter(|places| *places <= Decimal::MAX_SCALE)?;
        // floor(log10(price)) from exact base-10 components.
        let exponent =
            i64::from(price.mantissa().unsigned_abs().ilog10()) - i64::from(price.scale());
        Some((4 - exponent).max(0).min(i64::from(max_decimals)) as u32)
    }

    pub fn tick(self, price: Decimal) -> Option<Decimal> {
        Some(Decimal::new(1, self.decimal_places(price)?))
    }

    pub fn round_nearest(self, price: Decimal) -> Option<Decimal> {
        self.round(price, RoundingStrategy::MidpointTowardZero)
    }

    pub fn round_for_side(self, side: Side, price: Decimal, conservative: bool) -> Option<Decimal> {
        let strategy = match (side, conservative) {
            (Side::Bid, false) | (Side::Ask, true) => RoundingStrategy::ToPositiveInfinity,
            (Side::Bid, true) | (Side::Ask, false) => RoundingStrategy::ToNegativeInfinity,
        };
        self.round(price, strategy)
    }

    pub fn accepts(self, price: Decimal) -> bool {
        self.decimal_places(price)
            .is_some_and(|places| price.normalize().scale() <= places)
    }

    fn round(self, price: Decimal, strategy: RoundingStrategy) -> Option<Decimal> {
        let price = price
            .round_dp_with_strategy(self.decimal_places(price)?, strategy)
            .normalize();
        (price > Decimal::ZERO).then_some(price)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Size(pub Decimal);

impl Size {
    pub fn truncate(self, size_decimals: u32) -> anyhow::Result<Self> {
        let value = self
            .0
            .round_dp_with_strategy(size_decimals, RoundingStrategy::ToZero)
            .normalize();
        anyhow::ensure!(value > Decimal::ZERO, "invalid size after rounding");
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeInForce {
    Alo,
    Ioc,
    Gtc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum OrderGrouping {
    Na,
    NormalTpsl,
    PositionTpsl,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TpSl {
    Tp,
    Sl,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Action {
    Order(BatchOrder),
    BatchModify(BatchModify),
    Cancel(BatchCancel),
    CancelByCloid(BatchCancelCloid),
    TwapOrder(TwapOrderAction),
    TwapCancel(TwapCancelAction),
    UpdateLeverage(UpdateLeverageAction),
    UpdateIsolatedMargin(UpdateIsolatedMarginAction),
    AgentSetAbstraction(AgentSetAbstraction),
    ReserveRequestWeight { weight: u64 },
    Noop,
}

impl Action {
    pub fn hash(
        &self,
        nonce: u64,
        vault_address: Option<Address>,
        expires_after: Option<u64>,
    ) -> Result<B256, rmp_serde::encode::Error> {
        let mut bytes = rmp_serde::to_vec_named(self)?;
        bytes.extend(nonce.to_be_bytes());
        match vault_address {
            Some(address) => {
                bytes.push(1);
                bytes.extend(address.as_slice());
            }
            None => bytes.push(0),
        }
        if let Some(expires_after) = expires_after {
            bytes.push(0);
            bytes.extend(expires_after.to_be_bytes());
        }
        Ok(B256::from(keccak256(bytes)))
    }

    pub fn sign<S: SignerSync>(
        self,
        signer: &S,
        nonce: u64,
        vault_address: Option<Address>,
        expires_after: Option<u64>,
        chain: Chain,
    ) -> anyhow::Result<ActionRequest> {
        let agent = Agent {
            source: chain.source().to_owned(),
            connectionId: self.hash(nonce, vault_address, expires_after)?,
        };
        let signature = signer.sign_typed_data_sync(&agent, &core_domain())?;
        Ok(ActionRequest {
            action: self,
            nonce,
            signature: signature.into(),
            vault_address,
            expires_after,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ActionRequest {
    pub action: Action,
    pub nonce: u64,
    pub signature: Signature,
    pub vault_address: Option<Address>,
    pub expires_after: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BatchOrder {
    pub orders: Vec<OrderRequest>,
    pub grouping: OrderGrouping,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "b")]
    pub is_buy: bool,
    #[serde(rename = "p", with = "wire_decimal")]
    pub price: Decimal,
    #[serde(rename = "s", with = "wire_decimal")]
    pub size: Decimal,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "t")]
    pub order_type: OrderType,
    #[serde(rename = "c")]
    pub cloid: Cloid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum OrderType {
    Limit {
        tif: TimeInForce,
    },
    #[serde(rename_all = "camelCase")]
    Trigger {
        is_market: bool,
        #[serde(with = "wire_decimal")]
        trigger_px: Decimal,
        tpsl: TpSl,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BatchModify {
    pub modifies: Vec<Modify>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Modify {
    pub oid: OrderTarget,
    pub order: OrderRequest,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderTarget {
    Oid(u64),
    Cloid(Cloid),
}

impl Serialize for OrderTarget {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Oid(oid) => serializer.serialize_u64(*oid),
            Self::Cloid(cloid) => serializer.serialize_str(&cloid.to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for OrderTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TargetVisitor;

        impl<'de> de::Visitor<'de> for TargetVisitor {
            type Value = OrderTarget;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("u64 oid or 0x-prefixed 16-byte cloid")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(OrderTarget::Oid(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Cloid::from_str(value)
                    .map(OrderTarget::Cloid)
                    .map_err(E::custom)
            }
        }

        deserializer.deserialize_any(TargetVisitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BatchCancel {
    pub cancels: Vec<Cancel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Cancel {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "o")]
    pub oid: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BatchCancelCloid {
    pub cancels: Vec<CancelByCloid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelByCloid {
    pub asset: u32,
    pub cloid: Cloid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TwapOrderAction {
    pub twap: TwapOrder,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TwapOrder {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "b")]
    pub is_buy: bool,
    #[serde(rename = "s", with = "wire_decimal")]
    pub size: Decimal,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "m")]
    pub minutes: u64,
    #[serde(rename = "t")]
    pub randomize: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TwapCancelAction {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "t")]
    pub twap_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateLeverageAction {
    pub asset: u32,
    pub is_cross: bool,
    pub leverage: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIsolatedMarginAction {
    pub asset: u32,
    pub is_buy: bool,
    pub ntli: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentSetAbstraction {
    pub abstraction: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cloid([u8; 16]);

impl Cloid {
    pub const ZERO: Self = Self([0; 16]);

    pub fn from_u128(value: u128) -> anyhow::Result<Self> {
        anyhow::ensure!(value != 0, "cloid must be nonzero");
        Ok(Self::from_u128_unchecked(value))
    }

    pub fn from_u128_unchecked(value: u128) -> Self {
        debug_assert_ne!(value, 0);
        Self(value.to_be_bytes())
    }

    pub fn from_uuid_text(raw: &str) -> anyhow::Result<Self> {
        let compact = raw.replace('-', "");
        Self::from_hex(&compact)
    }

    fn from_hex(raw: &str) -> anyhow::Result<Self> {
        let hex = raw.strip_prefix("0x").unwrap_or(raw);
        anyhow::ensure!(hex.len() == 32, "cloid must be 16 bytes");
        let mut bytes = [0_u8; 16];
        for i in 0..16 {
            bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)?;
        }
        Ok(Self(bytes))
    }
}

impl Default for Cloid {
    fn default() -> Self {
        Self::ZERO
    }
}

impl FromStr for Cloid {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::from_hex(raw)
    }
}

impl fmt::Display for Cloid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("0x")?;
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for Cloid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Cloid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_str(&String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature {
    #[serde(serialize_with = "hex_u256", deserialize_with = "de_hex_u256")]
    pub r: U256,
    #[serde(serialize_with = "hex_u256", deserialize_with = "de_hex_u256")]
    pub s: U256,
    pub v: u64,
}

impl From<alloy_signer::Signature> for Signature {
    fn from(value: alloy_signer::Signature) -> Self {
        Self {
            r: value.r(),
            s: value.s(),
            v: value.recid().to_byte() as u64 + 27,
        }
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signature")
            .field("r", &format_args!("0x{:064x}", self.r))
            .field("s", &format_args!("0x{:064x}", self.s))
            .field("v", &self.v)
            .finish()
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:064x}{:064x}{:02x}", self.r, self.s, self.v)
    }
}

fn hex_u256<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("0x{value:064x}"))
}

fn de_hex_u256<'de, D>(deserializer: D) -> Result<U256, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    U256::from_str_radix(raw.strip_prefix("0x").unwrap_or(&raw), 16).map_err(de::Error::custom)
}

fn core_domain() -> Eip712Domain {
    eip712_domain! {
        name: "Exchange",
        version: "1",
        chain_id: 1337,
        verifying_contract: Address::ZERO,
    }
}

mod wire_decimal {
    use super::*;

    pub fn serialize<S>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.normalize().to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: Deserializer<'de>,
    {
        Decimal::from_str(&String::deserialize(deserializer)?)
            .map(|value| value.normalize())
            .map_err(de::Error::custom)
    }
}

sol! {
    #[derive(Debug, Serialize)]
    struct Agent {
        string source;
        bytes32 connectionId;
    }
}
