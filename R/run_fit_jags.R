# R/run_fit_jags.R
# PK/PD Analysis using JAGS with comprehensive error handling
# Modified for robust computation stability

# Load required libraries
library(rjags)
library(coda)
library(dplyr)
library(tidyr)

# Source safety wrapper if available
if (file.exists("R/safe_computation.R")) {
  source("R/safe_computation.R")
}

#' Run JAGS fitting with comprehensive error handling
#'
#' @param data Data frame with columns: ID, TIME, DV, DOSE, optional: AMT, EVID, MDV, Covariates
#' @param model Model string or model type identifier
#' @param params Parameters to monitor
#' @param config List with model configuration
#' @param progress_callback Optional function for progress updates
#'
#' @return List containing model results, parameters, predictions, and diagnostics
#' @export
run_fit_jags <- function(data, model, params, config, progress_callback = NULL) {
  
  # Validate inputs first
  tryCatch({
    validate_jags_inputs(data, config)
  }, error = function(e) {
    stop(sprintf("Input validation failed: %s", e$message))
  })
  
  # Set default configuration values
  config <- set_default_jags_config(config)
  
  # Prepare JAGS parameters
  jags_params <- list(
    data = data,
    model = model,
    params = params,
    n_chains = config$n_chains %||% 3,
    n_burnin = config$n_burnin %||% 1000,
    n_iter = config$n_iter %||% 5000,
    thin = config$thin %||% 1
  )
  
  # JAGS computation function with comprehensive error handling
  jags_computation <- function(p) {
    
    # Prepare data for JAGS
    jags_data <- prepare_jags_data(p$data, config)
    
    # Create or get model string
    if (is.character(p$model) && nchar(p$model) < 100) {
      # Assume it's a model type identifier
      model_string <- create_jags_model(p$model, config$error_model %||% "additive", jags_data)
    } else {
      # Assume it's the actual model string
      model_string <- p$model
    }
    
    # Stage 1: Model compilation with error handling
    if (!is.null(progress_callback)) {
      progress_callback("Compiling JAGS model...")
    }
    
    model_compiled <- tryCatch({
      jags.model(
        textConnection(model_string),
        data = jags_data,
        n.chains = p$n_chains,
        quiet = TRUE
      )
    }, error = function(e) {
      if (grepl("initial values", e$message, ignore.case = TRUE)) {
        # Try with custom initial values
        inits <- create_initial_values(config, jags_data, p$data)
        jags.model(
          textConnection(model_string),
          data = jags_data,
          inits = inits,
          n.chains = p$n_chains,
          quiet = TRUE
        )
      } else {
        stop(sprintf("Model compilation failed: %s", e$message))
      }
    })
    
    # Stage 2: Burn-in with error handling
    if (!is.null(progress_callback)) {
      progress_callback("Running burn-in phase...")
    }
    
    tryCatch({
      update(model_compiled, n.iter = p$n_burnin, progress.bar = "none")
    }, error = function(e) {
      # Try with reduced burn-in if memory issues
      if (grepl("memory|alloc", e$message, ignore.case = TRUE)) {
        warning("Memory issue during burn-in, reducing iterations")
        update(model_compiled, n.iter = max(100, p$n_burnin / 10), progress.bar = "none")
      } else {
        stop(sprintf("Burn-in failed: %s", e$message))
      }
    })
    
    # Stage 3: Sampling with error handling
    if (!is.null(progress_callback)) {
      progress_callback("Sampling from posterior...")
    }
    
    samples <- tryCatch({
      coda.samples(
        model_compiled,
        variable.names = p$params,
        n.iter = p$n_iter,
        thin = p$thin,
        progress.bar = "none"
      )
    }, error = function(e) {
      if (grepl("memory|alloc", e$message, ignore.case = TRUE)) {
        # Try with reduced iterations
        warning("Memory issue during sampling, reducing iterations")
        coda.samples(
          model_compiled,
          variable.names = p$params,
          n.iter = max(1000, p$n_iter / 5),
          thin = max(1, p$thin * 2),
          progress.bar = "none"
        )
      } else {
        stop(sprintf("Sampling failed: %s", e$message))
      }
    })
    
    return(list(
      samples = samples,
      model = model_compiled,
      jags_data = jags_data
    ))
  }
  
  # Run with safety wrapper if available
  if (exists("safe_bayesian_computation") && is.function(safe_bayesian_computation)) {
    result <- safe_bayesian_computation(
      computation_type = "jags",
      computation_fn = jags_computation,
      params = jags_params,
      session = getDefaultReactiveDomain()
    )
    
    if (!result$success) {
      # User-friendly error message with suggestions
      error_msg <- result$error
      
      if (grepl("initial values", error_msg, ignore.case = TRUE)) {
        error_msg <- paste(error_msg, 
          "\nSuggestions:",
          "\n- Check that priors are properly specified",
          "\n- Verify data is within reasonable ranges",
          "\n- Consider providing explicit initial values")
      } else if (grepl("memory", error_msg, ignore.case = TRUE)) {
        error_msg <- paste(error_msg,
          "\nSuggestions:",
          "\n- Reduce number of iterations or chains",
          "\n- Increase thinning interval",
          "\n- Close other applications to free memory")
      } else if (grepl("syntax", error_msg, ignore.case = TRUE)) {
        error_msg <- paste(error_msg,
          "\nSuggestion: Check JAGS model syntax")
      }
      
      stop(sprintf("JAGS computation failed: %s", error_msg))
    }
    
    computation_result <- result$result
    computation_diagnostics <- result$diagnostics
    computation_warnings <- result$warnings
    computation_time <- result$duration
    
  } else {
    # Fallback: direct execution with basic error handling
    computation_result <- tryCatch({
      jags_computation(jags_params)
    }, error = function(e) {
      stop(sprintf("JAGS computation failed: %s", e$message))
    })
    computation_diagnostics <- NULL
    computation_warnings <- character()
    computation_time <- NA
  }
  
  # Process results
  if (!is.null(progress_callback)) {
    progress_callback("Processing results...")
  }
  
  samples <- computation_result$samples
  model_compiled <- computation_result$model
  jags_data <- computation_result$jags_data
  
  # Calculate parameter statistics with error handling
  param_stats <- tryCatch({
    calculate_parameter_statistics(samples, config$model_type %||% "generic")
  }, error = function(e) {
    warning(sprintf("Could not calculate parameter statistics: %s", e$message))
    NULL
  })
  
  # Generate predictions with error handling
  predictions <- tryCatch({
    calculate_predictions(samples, data, config, jags_data)
  }, error = function(e) {
    warning(sprintf("Could not generate predictions: %s", e$message))
    NULL
  })
  
  # Calculate diagnostics
  if (is.null(computation_diagnostics)) {
    diagnostics <- tryCatch({
      calculate_jags_diagnostics(samples, model_compiled, jags_data)
    }, error = function(e) {
      warning(sprintf("Could not calculate diagnostics: %s", e$message))
      list(convergence = "unknown")
    })
  } else {
    diagnostics <- computation_diagnostics
  }
  
  if (!is.null(progress_callback)) {
    progress_callback("Fitting completed successfully!")
  }
  
  # Return formatted results
  return(list(
    model = model_compiled,
    parameters = param_stats,
    predictions = predictions,
    diagnostics = diagnostics,
    samples = samples,
    config = config,
    data = data,
    warnings = computation_warnings,
    computation_time = computation_time
  ))
}

#' Validate JAGS inputs
validate_jags_inputs <- function(data, config) {
  # Check required columns
  required_cols <- c("ID", "TIME", "DV")
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0) {
    stop(sprintf("Missing required columns: %s", paste(missing_cols, collapse = ", ")))
  }
  
  # Check for dose information
  if (!("DOSE" %in% names(data)) && !("AMT" %in% names(data))) {
    stop("Data must contain either DOSE or AMT column for dosing information")
  }
  
  # Check for invalid values
  if (any(is.na(data$TIME)) || any(data$TIME < 0)) {
    stop("Invalid TIME values detected (NA or negative)")
  }
  
  if (all(is.na(data$DV))) {
    stop("All DV values are NA")
  }
  
  # Validate configuration
  if (!is.null(config$n_chains) && (config$n_chains < 1 || config$n_chains > 10)) {
    stop("Number of chains must be between 1 and 10")
  }
  
  if (!is.null(config$n_iter) && config$n_iter < 100) {
    stop("Number of iterations must be at least 100")
  }
  
  return(TRUE)
}

#' Set default JAGS configuration
set_default_jags_config <- function(config) {
  defaults <- list(
    n_chains = 3,
    n_iter = 5000,
    n_burnin = 1000,
    thin = 1,
    adapt = 1000,
    error_model = "additive",
    model_type = "one_comp"
  )
  
  for (name in names(defaults)) {
    if (is.null(config[[name]])) {
      config[[name]] <- defaults[[name]]
    }
  }
  
  return(config)
}

#' Prepare data for JAGS
prepare_jags_data <- function(data, config) {
  # Handle missing values
  data <- data %>%
    filter(!is.na(TIME))
  
  # Handle BLQ data if present
  if ("BLQ" %in% names(data) || any(data$DV <= 0, na.rm = TRUE)) {
    lloq <- config$lloq %||% min(data$DV[data$DV > 0], na.rm = TRUE)
    data <- handle_blq_data(data, lloq)
  }
  
  # Get dose information
  if ("DOSE" %in% names(data)) {
    doses <- data$DOSE
  } else if ("AMT" %in% names(data)) {
    dose_data <- data %>%
      filter(!is.na(AMT), AMT > 0) %>%
      group_by(ID) %>%
      slice(1) %>%
      select(ID, DOSE = AMT)
    
    data <- data %>%
      left_join(dose_data, by = "ID")
    doses <- data$DOSE
  }
  
  # Create numeric ID mapping
  id_map <- data.frame(
    ID_original = unique(data$ID),
    ID_numeric = seq_along(unique(data$ID))
  )
  
  data <- data %>%
    left_join(id_map, by = c("ID" = "ID_original"))
  
  # Prepare JAGS data list
  jags_data <- list(
    N = nrow(data),
    n_subjects = length(unique(data$ID_numeric)),
    ID = data$ID_numeric,
    TIME = data$TIME,
    DV = data$DV,
    DOSE = doses
  )
  
  # Add BLQ information if present
  if ("BLQ" %in% names(data)) {
    jags_data$is_blq <- as.integer(data$BLQ)
    jags_data$lloq <- config$lloq %||% min(data$DV[data$DV > 0], na.rm = TRUE)
  }
  
  # Add covariates if present
  covariate_cols <- c("WT", "AGE", "SEX", "CRCL")
  for (cov in covariate_cols) {
    if (cov %in% names(data)) {
      jags_data[[cov]] <- data[[cov]]
    }
  }
  
  return(jags_data)
}

#' Create JAGS model string
create_jags_model <- function(model_type, error_model, jags_data) {
  # Basic one-compartment model as default
  model_string <- "
model {
  # Likelihood
  for (i in 1:N) {
    "
  
  # Add error model
  if (error_model == "additive") {
    model_string <- paste0(model_string, "
    DV[i] ~ dnorm(pred[i], tau)
    pred[i] <- IPRED[i]")
  } else if (error_model == "proportional") {
    model_string <- paste0(model_string, "
    DV[i] ~ dnorm(pred[i], tau_prop[i])
    tau_prop[i] <- tau / (pred[i] * pred[i] + 0.0001)
    pred[i] <- IPRED[i]")
  } else if (error_model == "combined") {
    model_string <- paste0(model_string, "
    DV[i] ~ dnorm(pred[i], tau_comb[i])
    tau_comb[i] <- 1 / (sigma_add * sigma_add + sigma_prop * sigma_prop * pred[i] * pred[i] + 0.0001)
    pred[i] <- IPRED[i]")
  }
  
  # Add PK model
  model_string <- paste0(model_string, "
    
    # One-compartment model
    ke[i] <- CL_ind[ID[i]] / V_ind[ID[i]]
    IPRED[i] <- DOSE[ID[i]] / V_ind[ID[i]] * exp(-ke[i] * TIME[i])
  }
  
  # Individual parameters
  for (j in 1:n_subjects) {
    log_CL_ind[j] ~ dnorm(log_CL_pop, tau_CL)
    log_V_ind[j] ~ dnorm(log_V_pop, tau_V)
    CL_ind[j] <- exp(log_CL_ind[j])
    V_ind[j] <- exp(log_V_ind[j])
  }
  
  # Population parameters with informative priors
  log_CL_pop ~ dnorm(log(5), 0.5)
  log_V_pop ~ dnorm(log(50), 0.5)
  CL_pop <- exp(log_CL_pop)
  V_pop <- exp(log_V_pop)
  
  # Between-subject variability
  tau_CL ~ dgamma(0.01, 0.01)
  tau_V ~ dgamma(0.01, 0.01)
  omega_CL <- 1 / tau_CL
  omega_V <- 1 / tau_V
  ")
  
  # Add error model parameters
  if (error_model == "additive") {
    model_string <- paste0(model_string, "
  # Residual error
  tau <- 1 / (sigma * sigma)
  sigma ~ dunif(0, 10)
}")
  } else if (error_model == "proportional") {
    model_string <- paste0(model_string, "
  # Residual error  
  tau <- 1 / (sigma_prop * sigma_prop)
  sigma_prop ~ dunif(0, 1)
  sigma <- sigma_prop
}")
  } else if (error_model == "combined") {
    model_string <- paste0(model_string, "
  # Residual error
  sigma_add ~ dunif(0, 10)
  sigma_prop ~ dunif(0, 1)
  sigma <- sigma_add
}")
  }
  
  return(model_string)
}

#' Create initial values for JAGS
create_initial_values <- function(config, jags_data, data) {
  n_chains <- config$n_chains
  n_subjects <- jags_data$n_subjects
  
  # Function to generate initial values for one chain
  generate_inits <- function() {
    list(
      log_CL_pop = rnorm(1, log(5), 0.2),
      log_V_pop = rnorm(1, log(50), 0.2),
      tau_CL = rgamma(1, 1, 1),
      tau_V = rgamma(1, 1, 1),
      log_CL_ind = rnorm(n_subjects, log(5), 0.1),
      log_V_ind = rnorm(n_subjects, log(50), 0.1),
      sigma = runif(1, 0.1, 2)
    )
  }
  
  # Generate initial values for all chains
  inits <- lapply(1:n_chains, function(i) generate_inits())
  
  return(inits)
}

#' Calculate parameter statistics
calculate_parameter_statistics <- function(samples, model_type) {
  # Combine chains
  combined_samples <- do.call(rbind, samples)
  
  # Get population parameters
  pop_params <- c("CL_pop", "V_pop", "Q_pop", "V2_pop", "Ka_pop", "F_pop",
                 "omega_CL", "omega_V", "omega_Q", "omega_V2", "sigma")
  
  # Calculate statistics for each parameter
  param_stats <- data.frame()
  
  for (param in pop_params) {
    if (param %in% colnames(combined_samples)) {
      param_samples <- combined_samples[, param]
      
      stats <- data.frame(
        Parameter = param,
        Mean = mean(param_samples, na.rm = TRUE),
        Median = median(param_samples, na.rm = TRUE),
        SD = sd(param_samples, na.rm = TRUE),
        SE = sd(param_samples, na.rm = TRUE) / sqrt(length(param_samples)),
        CI_2.5 = quantile(param_samples, 0.025, na.rm = TRUE),
        CI_97.5 = quantile(param_samples, 0.975, na.rm = TRUE),
        stringsAsFactors = FALSE
      )
      
      param_stats <- rbind(param_stats, stats)
    }
  }
  
  rownames(param_stats) <- NULL
  return(param_stats)
}

#' Calculate predictions from MCMC samples
calculate_predictions <- function(samples, data, config, jags_data) {
  # Simplified prediction calculation
  predictions <- data.frame(
    ID = data$ID,
    TIME = data$TIME,
    PRED = NA,
    IPRED = NA,
    IPRED_CI_2.5 = NA,
    IPRED_CI_97.5 = NA
  )
  
  # Extract IPRED if available in samples
  combined_samples <- do.call(rbind, samples)
  ipred_cols <- grep("^IPRED\\[", colnames(combined_samples), value = TRUE)
  
  if (length(ipred_cols) > 0) {
    for (i in 1:min(nrow(data), length(ipred_cols))) {
      ipred_samples <- combined_samples[, ipred_cols[i]]
      predictions$IPRED[i] <- mean(ipred_samples, na.rm = TRUE)
      predictions$IPRED_CI_2.5[i] <- quantile(ipred_samples, 0.025, na.rm = TRUE)
      predictions$IPRED_CI_97.5[i] <- quantile(ipred_samples, 0.975, na.rm = TRUE)
      predictions$PRED[i] <- predictions$IPRED[i]  # Simplified
    }
  }
  
  return(predictions)
}

#' Calculate JAGS-specific diagnostics
calculate_jags_diagnostics <- function(samples, model, jags_data) {
  diagnostics <- list()
  
  # Calculate DIC if possible
  try({
    dic_result <- dic.samples(model, n.iter = 1000, type = "pD")
    diagnostics$DIC <- sum(dic_result$deviance) + sum(dic_result$penalty)
    diagnostics$pD <- sum(dic_result$penalty)
  }, silent = TRUE)
  
  # Calculate convergence diagnostics
  if (requireNamespace("coda", quietly = TRUE) && length(samples) > 1) {
    try({
      # Gelman-Rubin diagnostic
      gelman_result <- coda::gelman.diag(samples, multivariate = FALSE)
      diagnostics$Rhat <- gelman_result$psrf[,1]
      diagnostics$max_Rhat <- max(gelman_result$psrf[,1], na.rm = TRUE)
      
      # Effective sample size
      eff_size <- coda::effectiveSize(samples)
      diagnostics$min_eff_size <- min(eff_size, na.rm = TRUE)
      
      # Heidelberger-Welch test
      heidel_result <- coda::heidel.diag(samples)
      diagnostics$heidel_passed <- all(sapply(heidel_result, function(x) all(x[,"pvalue"] > 0.05)))
      
      # Overall convergence assessment
      diagnostics$convergence <- (diagnostics$max_Rhat < 1.1 && 
                                 diagnostics$min_eff_size > 400 &&
                                 diagnostics$heidel_passed)
    }, silent = TRUE)
  }
  
  # Add basic info
  diagnostics$n_subjects <- jags_data$n_subjects
  diagnostics$n_observations <- jags_data$N
  
  return(diagnostics)
}

#' Handle BLQ (Below Limit of Quantification) data
handle_blq_data <- function(data, lloq = NULL) {
  if (is.null(lloq)) {
    # Estimate LLOQ as minimum non-zero value
    lloq <- min(data$DV[data$DV > 0], na.rm = TRUE)
  }
  
  # Flag BLQ observations
  data$BLQ <- data$DV < lloq | is.na(data$DV)
  
  # Impute BLQ values as LLOQ/2 (simple approach)
  # In production, would use censored likelihood
  data$DV[data$BLQ & !is.na(data$DV)] <- lloq / 2
  
  attr(data, "lloq") <- lloq
  return(data)
}

#' Get default reactive domain helper
getDefaultReactiveDomain <- function() {
  if (requireNamespace("shiny", quietly = TRUE)) {
    try(shiny::getDefaultReactiveDomain(), silent = TRUE)
  } else {
    NULL
  }
}

# Null-coalescing operator
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}