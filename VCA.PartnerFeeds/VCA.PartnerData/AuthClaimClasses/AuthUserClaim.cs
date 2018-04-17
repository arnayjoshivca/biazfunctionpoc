using Newtonsoft.Json;

namespace VCA.PartnerData.AuthClaimClasses
{
    public class AuthUserClaim
    {
        [JsonProperty("typ")]
        public string Type { get; set; }
        [JsonProperty("val")]
        public string Value { get; set; }
    }
}
