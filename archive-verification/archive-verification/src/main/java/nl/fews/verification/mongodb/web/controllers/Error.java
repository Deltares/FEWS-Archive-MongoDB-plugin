package nl.fews.verification.mongodb.web.controllers;

import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class Error implements ErrorController {

	private final ErrorAttributes errorAttributes;

    public Error(ErrorAttributes errorAttributes) {
        this.errorAttributes = errorAttributes;
    }

	@RequestMapping("/error")
	public ModelAndView error(WebRequest webRequest)  {
		var errorAttributes = this.errorAttributes.getErrorAttributes(webRequest, ErrorAttributeOptions.defaults());
		errorAttributes.put("email", "Anonymous");

		var authentication = SecurityContextHolder.getContext().getAuthentication();
		if (authentication != null && authentication.isAuthenticated()) {
			var principal = authentication.getPrincipal();
			if (principal instanceof OidcUser)
				errorAttributes.put("email", ((OidcUser) principal).getUserInfo().getClaims().getOrDefault("email", "Anonymous"));
		}
		ModelAndView modelAndView = new ModelAndView("error");
        modelAndView.addObject("errorAttributes", errorAttributes);
        return modelAndView;
	}
}
