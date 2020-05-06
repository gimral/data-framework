package leap.data.framework.extension.oauth.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Token {
    @JsonProperty(value = "access_token")
    private String accessTokenValue;

    @JsonProperty(value = "expires_in")
    private String expiresIn;

    @JsonProperty(value = "refresh_token")
    private String refreshToken;

    @JsonProperty(value = "refresh_expires_in")
    private String refreshTokenExpiresIn;

    @JsonProperty(value = "token_type")
    private String tokenType;

    @JsonProperty(value = "grant_type")
    private String grantType;

    @JsonProperty(value = "not-before-policy")
    private String notBeforePolicy;

    @JsonIgnore()
    private long expireTimeMillis;


    public String getAccessTokenValue() {
        return accessTokenValue;
    }

//    public void setAccessTokenValue(String accessTokenValue) {
//        this.accessTokenValue = accessTokenValue;
//    }
//
//    public String getExpiresIn() {
//        return expiresIn;
//    }
//
//    public void setExpiresIn(String expiresIn) {
//        this.expiresIn = expiresIn;
//    }
//
//    public String getRefreshToken() {
//        return refreshToken;
//    }
//
//    public void setRefreshToken(String refreshToken) {
//        this.refreshToken = refreshToken;
//    }
//
//    public String getRefreshTokenExpiresIn() {
//        return refreshTokenExpiresIn;
//    }
//
//    public void setRefreshTokenExpiresIn(String refreshTokenExpiresIn) {
//        this.refreshTokenExpiresIn = refreshTokenExpiresIn;
//    }
//
//    public String getTokenType() {
//        return tokenType;
//    }
//
//    public void setTokenType(String tokenType) {
//        this.tokenType = tokenType;
//    }

    public boolean isExpired(){
        return expireTimeMillis <= System.currentTimeMillis();
    }

    public void setExpireTimeMillis(long createTimeMillis) {
        expireTimeMillis = Integer.parseInt(expiresIn) * 1000 + createTimeMillis - 10000;
    }

//    public String getNotBeforePolicy() {
//        return notBeforePolicy;
//    }
//
//    public void setNotBeforePolicy(String notBeforePolicy) {
//        this.notBeforePolicy = notBeforePolicy;
//    }
}
