package nl.fews.verification.mongodb.web;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.authority.mapping.GrantedAuthoritiesMapper;
import org.springframework.security.oauth2.core.oidc.user.OidcUserAuthority;
import org.springframework.security.oauth2.core.user.OAuth2UserAuthority;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.savedrequest.HttpSessionRequestCache;

import java.util.*;
import java.util.stream.Collectors;

@Configuration
@EnableWebSecurity
public class SecurityConfiguration{
	@Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
		var cache = new HttpSessionRequestCache();
		cache.setMatchingRequestParameterName(null);
        return http.authorizeHttpRequests(a -> a.requestMatchers("/**").hasRole("VERIFICATION_ADMIN").anyRequest().authenticated())
			.csrf(c -> c.ignoringRequestMatchers("/graphql/**")).requestCache(r -> r.requestCache(cache))
			.oauth2Login(o -> o.userInfoEndpoint(e -> e.userAuthoritiesMapper(authoritiesMapper()))).build();
    }

	@Bean
	protected GrantedAuthoritiesMapper authoritiesMapper() {
		return authorities -> authorities.stream()
			.flatMap(a -> ((a instanceof OidcUserAuthority) ? extractRoles(((OidcUserAuthority) a).getIdToken().getClaims()) : (a instanceof OAuth2UserAuthority) ? extractRoles(((OAuth2UserAuthority) a).getAttributes()) : new HashSet<GrantedAuthority>()).stream())
			.collect(Collectors.toSet());
	}

	private Set<GrantedAuthority> extractRoles(Map<String, Object> claims) {
		return claims.containsKey("roles") && claims.get("roles") instanceof List<?> ? ((List<?>) claims.get("roles")).stream()
			.map(r -> new SimpleGrantedAuthority(String.format("ROLE_%s", r)))
			.collect(Collectors.toSet()) : new HashSet<>();
	}
}
