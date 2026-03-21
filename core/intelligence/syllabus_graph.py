from __future__ import annotations

import re
from typing import Dict, Iterable, List


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(text or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "na"


def _unique(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        v = str(value or "").strip()
        if not v:
            continue
        key = v.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


_GENERIC_STRUCTURAL = [
    "constraint_decomposition",
    "case_partitioning",
    "symbolic_transformation",
    "dimensional_sanity_check",
    "boundary_analysis",
]

_GENERIC_TRAPS = [
    "sign_error",
    "domain_violation",
    "invalid_assumption",
    "premature_approximation",
    "unit_mismatch",
]

_MICRO_SUFFIX = [
    "fundamentals",
    "standard_forms",
    "boundary_cases",
    "multi_step_variants",
    "inverse_problems",
    "constraint_variants",
    "jee_shortcuts",
    "proof_style_forms",
]


def _expand_subtopics(unit_name: str, core_topics: List[str], seed_subtopics: List[str], minimum: int = 10) -> List[str]:
    subtopics = _unique(seed_subtopics)
    i = 0
    while len(subtopics) < minimum and core_topics:
        core = core_topics[i % len(core_topics)]
        suffix = _MICRO_SUFFIX[i % len(_MICRO_SUFFIX)]
        subtopics.append(f"{_slug(core)}_{suffix}")
        i += 1
    if len(subtopics) > 25:
        subtopics = subtopics[:25]
    return _unique(subtopics)


def _expand_structural(seed: List[str], reasoning_archetypes: List[str]) -> List[str]:
    merged = _unique(list(seed) + list(reasoning_archetypes) + _GENERIC_STRUCTURAL)
    return merged[:10]


def _expand_traps(seed: List[str]) -> List[str]:
    merged = _unique(list(seed) + _GENERIC_TRAPS)
    return merged[:10]


def _unit(
    core_topics: List[str],
    subtopics: List[str],
    structural_patterns: List[str],
    common_traps: List[str],
    prerequisite_units: List[str],
    reasoning_archetypes: List[str],
    tools: List[str],
    practical_tags: List[str] | None = None,
) -> Dict:
    return {
        "core_topics": _unique(core_topics),
        "subtopics": _expand_subtopics("unit", _unique(core_topics), subtopics, minimum=10),
        "structural_patterns": _expand_structural(structural_patterns, reasoning_archetypes),
        "common_traps": _expand_traps(common_traps),
        "prerequisite_units": _unique(prerequisite_units),
        "reasoning_archetypes": _unique(reasoning_archetypes),
        "tools": _unique(tools),
        "practical_tags": _unique(practical_tags or []),
    }


def build_syllabus_hierarchy() -> Dict[str, Dict[str, Dict]]:
    """
    Advanced JEE syllabus hierarchy:
    subject -> unit -> metadata
    """

    syllabus: Dict[str, Dict[str, Dict]] = {
        "Mathematics": {
            "Sets, Relations and Functions": _unit(
                core_topics=["set_operations", "relations", "functions", "composition", "inverse_function"],
                subtopics=["power_set", "equivalence_relation", "bijection", "domain_range", "modulus_function"],
                structural_patterns=["mapping_consistency", "set_partitioning", "inclusion_exclusion"],
                common_traps=["relation_vs_function_confusion", "domain_overlook", "many_to_one_misread"],
                prerequisite_units=[],
                reasoning_archetypes=["transformation_based_reasoning", "substitution_strategy", "invariant_method"],
                tools=["set_algebra", "logical_quantifiers"],
            ),
            "Complex Numbers and Quadratic Equations": _unit(
                core_topics=["argand_plane", "modulus_argument", "roots_of_quadratic", "conjugate_properties"],
                subtopics=["locus_in_complex_plane", "de_moivre_usage", "discriminant_analysis", "root_symmetry"],
                structural_patterns=["geometric_locus_construction", "symmetry_exploitation", "transformation_based_reasoning"],
                common_traps=["wrong_argument_branch", "conjugate_sign_error", "discriminant_misclassification"],
                prerequisite_units=["Sets, Relations and Functions"],
                reasoning_archetypes=["symmetry_exploitation", "substitution_strategy", "geometric_locus_construction"],
                tools=["algebraic_identity", "polar_form"],
            ),
            "Matrices and Determinants": _unit(
                core_topics=["matrix_operations", "determinant_properties", "inverse_matrix", "system_of_equations"],
                subtopics=["rank_consistency", "adjoint_method", "row_reduction", "parameterized_systems"],
                structural_patterns=["row_operation_invariance", "determinant_transformation", "extremal_argument"],
                common_traps=["illegal_row_operation", "determinant_scaling_error", "inverse_existence_misread"],
                prerequisite_units=["Sets, Relations and Functions"],
                reasoning_archetypes=["transformation_based_reasoning", "invariant_method", "extremal_argument"],
                tools=["linear_algebra", "equation_solving"],
            ),
            "Permutations and Combinations": _unit(
                core_topics=["permutation", "combination", "distribution", "arrangements_under_constraints"],
                subtopics=[
                    "circular_arrangements",
                    "derangements",
                    "identical_object_distribution",
                    "constrained_selection",
                    "generating_function_methods",
                ],
                structural_patterns=["exclusion_principle", "complementary_counting", "arrangement_under_restriction"],
                common_traps=["overcounting", "double_counting", "adjacency_confusion"],
                prerequisite_units=["Sets, Relations and Functions", "Binomial Theorem and Sequence & Series"],
                reasoning_archetypes=["transformation_based_reasoning", "extremal_argument", "generating_function_strategy"],
                tools=["counting", "set_algebra"],
            ),
            "Binomial Theorem and Sequence & Series": _unit(
                core_topics=["binomial_expansion", "general_term", "arithmetic_progression", "geometric_progression"],
                subtopics=["coefficient_comparison", "middle_term_logic", "telescoping_series", "sum_of_terms"],
                structural_patterns=["symmetry_exploitation", "term_index_alignment", "series_expansion_transform"],
                common_traps=["wrong_term_index", "middle_term_parity_error", "ratio_assumption_error"],
                prerequisite_units=["Permutations and Combinations"],
                reasoning_archetypes=["symmetry_exploitation", "transformation_based_reasoning", "substitution_strategy"],
                tools=["combinatorics", "algebraic_expansion"],
            ),
            "Trigonometry": _unit(
                core_topics=["trigonometric_identities", "equations", "inverse_trigonometric_functions", "properties_of_triangles"],
                subtopics=["multiple_angle_transform", "periodicity_handling", "principal_value_cases", "triangle_simplification"],
                structural_patterns=["identity_transformation", "periodicity_partitioning", "symmetry_exploitation"],
                common_traps=["principal_value_error", "period_mismatch", "quadrant_sign_error"],
                prerequisite_units=["Sets, Relations and Functions"],
                reasoning_archetypes=["transformation_based_reasoning", "symmetry_exploitation", "substitution_strategy"],
                tools=["trig_identities", "graph_symmetry"],
            ),
            "Coordinate Geometry": _unit(
                core_topics=["straight_line", "circle", "parabola", "ellipse", "hyperbola"],
                subtopics=["tangent_normal_form", "director_circle", "focal_property_usage", "parametric_forms"],
                structural_patterns=["geometric_locus_construction", "parameter_elimination", "symmetry_exploitation"],
                common_traps=["slope_sign_error", "focus_directrix_mismatch", "parameter_domain_loss"],
                prerequisite_units=["Complex Numbers and Quadratic Equations", "Trigonometry"],
                reasoning_archetypes=["geometric_locus_construction", "transformation_based_reasoning", "symmetry_exploitation"],
                tools=["analytic_geometry", "algebraic_elimination"],
            ),
            "Vector Algebra": _unit(
                core_topics=["dot_product", "cross_product", "vector_projection", "direction_cosines"],
                subtopics=["coplanarity_test", "line_plane_angle", "vector_triple_product", "vector_equation_forms"],
                structural_patterns=["projection_decomposition", "invariant_method", "symmetry_exploitation"],
                common_traps=["right_hand_rule_error", "projection_sign_error", "unit_vector_misuse"],
                prerequisite_units=["Coordinate Geometry"],
                reasoning_archetypes=["transformation_based_reasoning", "invariant_method", "symmetry_exploitation"],
                tools=["vector_tools", "coordinate_systems"],
            ),
            "Three Dimensional Geometry": _unit(
                core_topics=["line_in_3d", "plane", "distance_between_entities", "angle_between_entities"],
                subtopics=["skew_lines", "shortest_distance", "plane_intersection", "direction_ratio_constraints"],
                structural_patterns=["vector_reduction", "projection_based_reasoning", "geometric_locus_construction"],
                common_traps=["skew_distance_formula_error", "normal_vector_confusion", "direction_ratio_scaling"],
                prerequisite_units=["Vector Algebra", "Coordinate Geometry"],
                reasoning_archetypes=["substitution_strategy", "geometric_locus_construction", "energy_method_substitution"],
                tools=["vector_tools", "distance_metrics"],
            ),
            "Limits, Continuity and Differentiability": _unit(
                core_topics=["limits", "continuity", "differentiability", "indeterminate_forms"],
                subtopics=["lhopital_rule", "taylor_series_expansion", "squeeze_theorem", "dominant_term_analysis"],
                structural_patterns=["asymptotic_comparison", "series_expansion_transform", "transformation_based_reasoning"],
                common_traps=["invalid_cancellation", "wrong_order_expansion", "domain_loss"],
                prerequisite_units=["Binomial Theorem and Sequence & Series", "Trigonometry"],
                reasoning_archetypes=["transformation_based_reasoning", "extremal_argument", "substitution_strategy"],
                tools=["calculus", "series"],
            ),
            "Integral Calculus and Differential Equations": _unit(
                core_topics=["indefinite_integral", "definite_integral", "area_under_curve", "differential_equation"],
                subtopics=["integration_by_parts", "partial_fraction", "substitution_integral", "first_order_linear_de"],
                structural_patterns=["substitution_strategy", "boundary_analysis", "invariant_method"],
                common_traps=["missing_constant", "limit_substitution_error", "separable_form_misread"],
                prerequisite_units=["Limits, Continuity and Differentiability"],
                reasoning_archetypes=["substitution_strategy", "transformation_based_reasoning", "extremal_argument"],
                tools=["calculus", "algebraic_manipulation"],
            ),
            "Probability and Statistics": _unit(
                core_topics=["conditional_probability", "bayes_theorem", "mean_variance", "distribution_basics"],
                subtopics=["independence_checks", "random_variable_transform", "binomial_distribution", "expectation_linearity"],
                structural_patterns=["case_partitioning", "complementary_counting", "invariant_method"],
                common_traps=["independent_vs_mutually_exclusive", "sample_space_bias", "incorrect_conditioning"],
                prerequisite_units=["Permutations and Combinations", "Sets, Relations and Functions"],
                reasoning_archetypes=["invariant_method", "extremal_argument", "generating_function_strategy"],
                tools=["probability_tools", "combinatorics"],
            ),
        },
        "Physics": {
            "Units, Dimensions and Error Analysis": _unit(
                core_topics=["dimensional_analysis", "significant_figures", "measurement_errors", "uncertainty_propagation"],
                subtopics=["least_count", "percentage_error", "dimensional_homogeneity", "precision_vs_accuracy"],
                structural_patterns=["dimensional_sanity_check", "error_propagation_chain", "constraint_decomposition"],
                common_traps=["unit_mismatch", "significant_digit_misrounding", "relative_error_confusion"],
                prerequisite_units=[],
                reasoning_archetypes=["invariant_method", "transformation_based_reasoning", "substitution_strategy"],
                tools=["unit_systems", "error_analysis"],
                practical_tags=["experimental_setup", "instrument_calibration", "error_analysis"],
            ),
            "Kinematics": _unit(
                core_topics=["motion_in_1d", "motion_in_2d", "relative_motion", "projectile_motion"],
                subtopics=["time_elimination", "graph_interpretation", "frame_transform", "constraint_kinematics"],
                structural_patterns=["component_decomposition", "graph_based_reasoning", "transformation_based_reasoning"],
                common_traps=["sign_convention_error", "frame_mismatch", "projectile_time_confusion"],
                prerequisite_units=["Units, Dimensions and Error Analysis"],
                reasoning_archetypes=["transformation_based_reasoning", "substitution_strategy", "symmetry_exploitation"],
                tools=["vector_tools", "calculus"],
            ),
            "Laws of Motion and Friction": _unit(
                core_topics=["newton_laws", "free_body_diagram", "friction", "pseudo_force"],
                subtopics=["inclined_plane_systems", "pulley_constraints", "non_inertial_frames", "limiting_friction"],
                structural_patterns=["constraint_decomposition", "force_balance", "invariant_method"],
                common_traps=["missing_reaction_component", "wrong_friction_direction", "pseudo_force_omission"],
                prerequisite_units=["Kinematics"],
                reasoning_archetypes=["transformation_based_reasoning", "invariant_method", "extremal_argument"],
                tools=["vector_tools", "force_balance"],
            ),
            "Work, Energy and Power": _unit(
                core_topics=["work_energy_theorem", "potential_energy", "power", "conservative_forces"],
                subtopics=["variable_force_work", "energy_graphs", "spring_block_system", "escape_velocity_link"],
                structural_patterns=["energy_method_substitution", "potential_landscape_analysis", "boundary_analysis"],
                common_traps=["wrong_reference_level", "work_sign_error", "non_conservative_force_ignore"],
                prerequisite_units=["Laws of Motion and Friction"],
                reasoning_archetypes=["energy_method_substitution", "symmetry_exploitation", "substitution_strategy"],
                tools=["energy_balance", "calculus"],
            ),
            "Rotational Motion": _unit(
                core_topics=["torque", "moment_of_inertia", "angular_momentum", "rolling_motion"],
                subtopics=["parallel_axis_usage", "rolling_constraints", "impulse_angular_momentum", "toppling_conditions"],
                structural_patterns=["rotational_translational_mapping", "invariant_method", "extremal_argument"],
                common_traps=["wrong_axis_choice", "rolling_without_slipping_misuse", "angular_linear_mixup"],
                prerequisite_units=["Work, Energy and Power"],
                reasoning_archetypes=["invariant_method", "energy_method_substitution", "extremal_argument"],
                tools=["vector_tools", "calculus", "energy_balance"],
            ),
            "Gravitation": _unit(
                core_topics=["newton_gravitation", "gravitational_potential", "satellite_motion", "escape_velocity"],
                subtopics=["orbital_energy", "geo_stationary_constraints", "field_superposition", "shell_theorem_usage"],
                structural_patterns=["inverse_square_reasoning", "energy_method_substitution", "symmetry_exploitation"],
                common_traps=["radius_vs_altitude_mixup", "sign_error_in_potential", "orbital_speed_formula_swap"],
                prerequisite_units=["Work, Energy and Power"],
                reasoning_archetypes=["symmetry_exploitation", "energy_method_substitution", "invariant_method"],
                tools=["calculus", "energy_balance"],
            ),
            "Thermodynamics and Kinetic Theory": _unit(
                core_topics=["first_law", "second_law", "ideal_gas", "kinetic_theory"],
                subtopics=["pv_diagrams", "cyclic_processes", "heat_capacity_relations", "rms_speed_logic"],
                structural_patterns=["state_variable_transformation", "process_path_analysis", "energy_method_substitution"],
                common_traps=["sign_convention_in_thermo", "path_function_confusion", "absolute_temp_error"],
                prerequisite_units=["Units, Dimensions and Error Analysis"],
                reasoning_archetypes=["transformation_based_reasoning", "energy_method_substitution", "invariant_method"],
                tools=["thermodynamics", "calculus", "probability_tools"],
            ),
            "Oscillations and Waves": _unit(
                core_topics=["simple_harmonic_motion", "wave_equation", "superposition", "resonance"],
                subtopics=["phase_relation", "energy_in_shm", "standing_waves", "beat_frequency"],
                structural_patterns=["phase_space_reasoning", "symmetry_exploitation", "extremal_argument"],
                common_traps=["phase_sign_error", "angular_frequency_mixup", "node_antinode_confusion"],
                prerequisite_units=["Kinematics"],
                reasoning_archetypes=["symmetry_exploitation", "substitution_strategy", "invariant_method"],
                tools=["trig_identities", "calculus"],
            ),
            "Electrostatics and Capacitance": _unit(
                core_topics=["coulomb_law", "electric_field", "gauss_law", "capacitance"],
                subtopics=["field_line_superposition", "dielectric_insertion", "energy_in_capacitor", "equipotential_surfaces"],
                structural_patterns=["symmetry_exploitation", "gaussian_surface_selection", "energy_method_substitution"],
                common_traps=["gauss_surface_mismatch", "series_parallel_capacitor_error", "field_potential_mixup"],
                prerequisite_units=["Vector Algebra", "Work, Energy and Power"],
                reasoning_archetypes=["symmetry_exploitation", "energy_method_substitution", "substitution_strategy"],
                tools=["vector_tools", "calculus", "field_theory"],
            ),
            "Current Electricity and Circuit Analysis": _unit(
                core_topics=["ohm_law", "kirchhoff_laws", "network_reduction", "meter_bridge"],
                subtopics=["internal_resistance", "balanced_bridge", "power_dissipation", "combination_networks"],
                structural_patterns=["constraint_decomposition", "loop_equation_reasoning", "invariant_method"],
                common_traps=["current_direction_assumption", "parallel_series_misread", "power_formula_mismatch"],
                prerequisite_units=["Electrostatics and Capacitance"],
                reasoning_archetypes=["transformation_based_reasoning", "invariant_method", "substitution_strategy"],
                tools=["linear_algebra", "circuit_theory"],
                practical_tags=["experimental_setup", "instrument_calibration", "error_analysis"],
            ),
            "Magnetism and Electromagnetic Induction": _unit(
                core_topics=["magnetic_field", "lorentz_force", "biot_savart", "faraday_law"],
                subtopics=["motion_in_magnetic_field", "induced_emf", "lenz_law_direction", "rlc_transients"],
                structural_patterns=["right_hand_rule_reasoning", "flux_variation_mapping", "energy_method_substitution"],
                common_traps=["direction_rule_error", "flux_sign_confusion", "induction_cause_effect_mixup"],
                prerequisite_units=["Current Electricity and Circuit Analysis", "Vector Algebra"],
                reasoning_archetypes=["invariant_method", "transformation_based_reasoning", "symmetry_exploitation"],
                tools=["vector_tools", "calculus", "field_theory"],
            ),
            "Optics and Modern Physics": _unit(
                core_topics=["ray_optics", "wave_optics", "photoelectric_effect", "nuclear_physics"],
                subtopics=["lens_combination", "interference_conditions", "diffraction", "de_broglie_relation"],
                structural_patterns=["path_difference_reasoning", "energy_quantization_transform", "symmetry_exploitation"],
                common_traps=["sign_convention_in_lens", "constructive_destructive_mixup", "units_in_quantum_formula"],
                prerequisite_units=["Oscillations and Waves", "Electrostatics and Capacitance"],
                reasoning_archetypes=["transformation_based_reasoning", "symmetry_exploitation", "extremal_argument"],
                tools=["wave_theory", "algebraic_manipulation"],
            ),
            "Experimental Skills (Physics)": _unit(
                core_topics=["vernier_calipers", "screw_gauge", "potentiometer", "meter_bridge", "graph_plotting"],
                subtopics=["least_count_correction", "zero_error_handling", "calibration_curve", "slope_interpretation"],
                structural_patterns=["experimental_setup", "instrument_calibration", "error_analysis"],
                common_traps=["least_count_ignore", "zero_error_sign", "graph_scale_bias"],
                prerequisite_units=["Units, Dimensions and Error Analysis"],
                reasoning_archetypes=["invariant_method", "substitution_strategy", "transformation_based_reasoning"],
                tools=["measurement", "error_analysis", "graph_analysis"],
                practical_tags=["experimental_setup", "instrument_calibration", "error_analysis"],
            ),
        },
        "Chemistry": {
            "Some Basic Concepts of Chemistry": _unit(
                core_topics=["mole_concept", "stoichiometry", "limiting_reagent", "empirical_formula"],
                subtopics=["mass_percent", "normality_molarity", "reaction_yield", "gas_stoichiometry"],
                structural_patterns=["ratio_proportion_reasoning", "constraint_decomposition", "dimensional_sanity_check"],
                common_traps=["mole_mass_mixup", "limiting_reagent_error", "unit_conversion_skip"],
                prerequisite_units=[],
                reasoning_archetypes=["substitution_strategy", "transformation_based_reasoning", "invariant_method"],
                tools=["algebraic_manipulation", "dimensional_analysis"],
            ),
            "Atomic Structure and Periodicity": _unit(
                core_topics=["quantum_numbers", "electronic_configuration", "periodic_trends", "effective_nuclear_charge"],
                subtopics=["aufbau_exceptions", "shielding_effect", "ionization_enthalpy_trends", "orbital_shapes"],
                structural_patterns=["trend_comparison", "symmetry_exploitation", "configuration_transform"],
                common_traps=["orbital_order_error", "exception_ignore", "trend_overgeneralization"],
                prerequisite_units=["Some Basic Concepts of Chemistry"],
                reasoning_archetypes=["transformation_based_reasoning", "symmetry_exploitation", "invariant_method"],
                tools=["periodic_table_logic", "electronic_structure"],
            ),
            "Chemical Bonding and Molecular Structure": _unit(
                core_topics=["ionic_covalent_bonding", "vsepr", "hybridization", "molecular_orbital_theory"],
                subtopics=["bond_order", "formal_charge", "resonance", "dipole_moment"],
                structural_patterns=["structure_property_mapping", "electron_count_invariance", "symmetry_exploitation"],
                common_traps=["hybridization_miscount", "formal_charge_sign", "geometry_vs_shape_confusion"],
                prerequisite_units=["Atomic Structure and Periodicity"],
                reasoning_archetypes=["symmetry_exploitation", "transformation_based_reasoning", "invariant_method"],
                tools=["electron_counting", "geometric_reasoning"],
            ),
            "States of Matter and Thermodynamics": _unit(
                core_topics=["gas_laws", "intermolecular_forces", "enthalpy", "entropy", "gibbs_energy"],
                subtopics=["real_gas_deviation", "phase_diagrams", "calorimetry", "spontaneity_conditions"],
                structural_patterns=["state_variable_transformation", "energy_method_substitution", "boundary_analysis"],
                common_traps=["sign_convention_confusion", "state_vs_path_variable", "unit_mismatch_heat"],
                prerequisite_units=["Some Basic Concepts of Chemistry"],
                reasoning_archetypes=["energy_method_substitution", "transformation_based_reasoning", "invariant_method"],
                tools=["thermodynamics", "calculus"],
            ),
            "Chemical Equilibrium and Ionic Equilibrium": _unit(
                core_topics=["equilibrium_constant", "le_chatelier", "acid_base_equilibrium", "solubility_product"],
                subtopics=["buffer_logic", "common_ion_effect", "ph_calculation", "hydrolysis"],
                structural_patterns=["constraint_decomposition", "approximation_validation", "invariant_method"],
                common_traps=["wrong_equilibrium_expression", "invalid_approximation", "logarithm_sign_error"],
                prerequisite_units=["States of Matter and Thermodynamics"],
                reasoning_archetypes=["substitution_strategy", "invariant_method", "extremal_argument"],
                tools=["logarithms", "thermodynamics", "algebraic_manipulation"],
            ),
            "Redox Reactions and Electrochemistry": _unit(
                core_topics=["oxidation_number", "balancing_redox", "electrochemical_cells", "nernst_equation"],
                subtopics=["equivalent_concept", "cell_potential", "electrolysis", "faraday_laws"],
                structural_patterns=["electron_balance_invariance", "potential_difference_reasoning", "constraint_decomposition"],
                common_traps=["oxidation_state_error", "electron_count_mismatch", "nernst_log_term_misuse"],
                prerequisite_units=["Some Basic Concepts of Chemistry", "Chemical Equilibrium and Ionic Equilibrium"],
                reasoning_archetypes=["invariant_method", "substitution_strategy", "transformation_based_reasoning"],
                tools=["algebraic_manipulation", "logarithms", "thermodynamics"],
            ),
            "Chemical Kinetics and Surface Chemistry": _unit(
                core_topics=["rate_law", "order_of_reaction", "activation_energy", "adsorption_isotherm"],
                subtopics=["integrated_rate_equations", "half_life_relations", "catalysis", "collision_theory"],
                structural_patterns=["rate_data_fit", "graph_based_reasoning", "transformation_based_reasoning"],
                common_traps=["order_vs_molecularity", "log_plot_misread", "rate_constant_unit_error"],
                prerequisite_units=["States of Matter and Thermodynamics"],
                reasoning_archetypes=["transformation_based_reasoning", "substitution_strategy", "extremal_argument"],
                tools=["graph_analysis", "calculus", "logarithms"],
            ),
            "Organic Chemistry: General Principles": _unit(
                core_topics=["inductive_effect", "resonance", "acidity_basicity", "reaction_intermediates"],
                subtopics=["stability_ordering", "hyperconjugation", "stereochemical_consequences", "mechanistic_steps"],
                structural_patterns=["mechanism_flow_tracking", "charge_stability_reasoning", "symmetry_exploitation"],
                common_traps=["wrong_intermediate", "resonance_misplacement", "stereochemistry_ignore"],
                prerequisite_units=["Chemical Bonding and Molecular Structure"],
                reasoning_archetypes=["transformation_based_reasoning", "symmetry_exploitation", "invariant_method"],
                tools=["electron_pushing", "acidity_scale"],
            ),
            "Hydrocarbons and Halo Compounds": _unit(
                core_topics=["alkanes_alkenes_alkynes", "aromaticity", "free_radical_reaction", "substitution_elimination"],
                subtopics=["markovnikov_logic", "anti_markovnikov_cases", "benzylic_reactivity", "rearrangement_patterns"],
                structural_patterns=["mechanism_branching", "reaction_path_selection", "substitution_strategy"],
                common_traps=["markovnikov_exception_ignore", "rearrangement_skip", "stability_order_error"],
                prerequisite_units=["Organic Chemistry: General Principles"],
                reasoning_archetypes=["transformation_based_reasoning", "substitution_strategy", "extremal_argument"],
                tools=["mechanism_analysis", "thermodynamics"],
            ),
            "Oxygen/Nitrogen Functional Groups and Biomolecules": _unit(
                core_topics=["alcohols_phenols_ethers", "aldehydes_ketones", "carboxylic_acids", "amines", "biomolecules"],
                subtopics=["name_reaction_mapping", "oxidation_reduction_paths", "protection_deprotection", "functional_group_interconversion"],
                structural_patterns=["functional_group_transform", "multi_step_synthesis_planning", "invariant_method"],
                common_traps=["wrong_reagent_choice", "chemoselectivity_misread", "name_reaction_confusion"],
                prerequisite_units=["Hydrocarbons and Halo Compounds"],
                reasoning_archetypes=["transformation_based_reasoning", "substitution_strategy", "invariant_method"],
                tools=["reaction_networks", "mechanism_analysis"],
            ),
            "Inorganic Chemistry: p-Block, d/f-Block, Coordination": _unit(
                core_topics=["periodic_reactivity", "coordination_compounds", "magnetic_properties", "color_and_spectra"],
                subtopics=["cfse_logic", "isomerism_in_coordination", "oxidation_states", "qualitative_reactions"],
                structural_patterns=["trend_comparison", "ligand_field_reasoning", "symmetry_exploitation"],
                common_traps=["oxidation_state_mixup", "ligand_strength_order_error", "isomer_count_error"],
                prerequisite_units=["Atomic Structure and Periodicity", "Chemical Bonding and Molecular Structure"],
                reasoning_archetypes=["symmetry_exploitation", "transformation_based_reasoning", "invariant_method"],
                tools=["periodic_table_logic", "coordination_theory"],
            ),
            "Practical Chemistry and Experimental Techniques": _unit(
                core_topics=["titration", "salt_analysis", "qualitative_tests", "laboratory_safety"],
                subtopics=["indicator_selection", "end_point_vs_equivalence", "volumetric_calculation", "error_sources"],
                structural_patterns=["experimental_setup", "instrument_calibration", "titration_logic", "error_analysis"],
                common_traps=["indicator_mismatch", "endpoint_overshoot", "normality_conversion_error"],
                prerequisite_units=["Chemical Equilibrium and Ionic Equilibrium", "Redox Reactions and Electrochemistry"],
                reasoning_archetypes=["substitution_strategy", "invariant_method", "transformation_based_reasoning"],
                tools=["volumetric_analysis", "error_analysis"],
                practical_tags=["experimental_setup", "instrument_calibration", "titration_logic", "error_analysis"],
            ),
        },
    }

    return syllabus


def flatten_syllabus_units(syllabus: Dict[str, Dict[str, Dict]]) -> List[Dict]:
    rows: List[Dict] = []
    for subject, units in syllabus.items():
        for unit_name, spec in units.items():
            rows.append(
                {
                    "subject": subject,
                    "unit": unit_name,
                    "core_topics": list(spec.get("core_topics", [])),
                    "subtopics": list(spec.get("subtopics", [])),
                    "structural_patterns": list(spec.get("structural_patterns", [])),
                    "common_traps": list(spec.get("common_traps", [])),
                    "prerequisite_units": list(spec.get("prerequisite_units", [])),
                    "reasoning_archetypes": list(spec.get("reasoning_archetypes", [])),
                    "tools": list(spec.get("tools", [])),
                    "practical_tags": list(spec.get("practical_tags", [])),
                }
            )
    return rows
