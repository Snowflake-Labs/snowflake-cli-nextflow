from groovy_parser.parser import parse_groovy_content, digest_lark_tree
from typing import Optional, Dict, Any, List
import os


class NextflowConfigParser:
    """
    Extracts Snowflake-related settings from a Nextflow config.

    Supported shapes:
    - snowflake { computePool = 'POOL'; workDirStage = 'MYSTAGE'; stageMounts = '...'; enableStageMountV2 = true }
    - snowflake.computePool = 'POOL' (also within profiles)
    - profiles { myprof { snowflake { ... } } }
    """

    def parse(self, project_dir: str, profile: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse the nextflow.config file and extract snowflake configuration and plugins.

        Args:
                project_dir: Directory containing nextflow.config file
                profile: Optional profile name to extract config from

        Returns:
                Dictionary containing snowflake configuration values and plugins information
        """
        config_path = os.path.join(project_dir, "nextflow.config")

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"nextflow.config file not found in {project_dir}")

        with open(config_path, "r") as f:
            config_text = f.read()

        try:
            tree = parse_groovy_content(config_text)
            t_tree = digest_lark_tree(tree)

            # Extract snowflake configuration
            snowflake_config = self._extract_snowflake_config(t_tree, profile)

            # Extract plugins configuration (always from global scope)
            plugins_config = self._extract_plugins_config(t_tree)

            # Combine both configurations
            result = snowflake_config.copy()
            if plugins_config:
                result["plugins"] = plugins_config

            return result

        except Exception as e:
            raise RuntimeError(f"Failed to parse nextflow.config: {e}")

    def _extract_snowflake_config(self, tree: Dict[str, Any], profile: Optional[str] = None) -> Dict[str, Any]:
        """Extract snowflake configuration from the parsed AST tree."""
        config = {}

        # First, look for global snowflake configuration
        config.update(self._extract_global_snowflake_config(tree))

        # If profile is specified, look for profile-specific configuration
        if profile:
            profile_config = self._extract_profile_snowflake_config(tree, profile)
            config.update(profile_config)  # Profile config overrides global

        return config

    def _extract_global_snowflake_config(self, tree: Dict[str, Any]) -> Dict[str, Any]:
        """Extract global snowflake configuration (not within profiles)."""
        config = {}

        # Look for statements in the script
        statements = self._get_statements(tree)

        for statement in statements:
            # Handle snowflake block: snowflake { ... }
            if self._is_snowflake_block(statement):
                block_config = self._extract_from_snowflake_block(statement)
                config.update(block_config)

            # Handle dot notation: snowflake.property = value
            elif self._is_snowflake_dot_notation(statement):
                prop_name, value = self._extract_from_dot_notation(statement)
                if prop_name and value is not None:
                    config[prop_name] = value

        return config

    def _extract_profile_snowflake_config(self, tree: Dict[str, Any], profile: str) -> Dict[str, Any]:
        """Extract snowflake configuration from a specific profile."""
        config = {}

        # Find profiles block
        statements = self._get_statements(tree)

        for statement in statements:
            if self._is_profiles_block(statement):
                # Look for the specific profile
                profile_block = self._find_profile_in_block(statement, profile)
                if profile_block:
                    # Extract snowflake config from this profile
                    profile_statements = self._get_statements_from_block(profile_block)

                    for prof_statement in profile_statements:
                        # Handle snowflake block within profile
                        if self._is_snowflake_block(prof_statement):
                            block_config = self._extract_from_snowflake_block(prof_statement)
                            config.update(block_config)

                        # Handle dot notation within profile
                        elif self._is_snowflake_dot_notation(prof_statement):
                            prop_name, value = self._extract_from_dot_notation(prof_statement)
                            if prop_name and value is not None:
                                config[prop_name] = value

        return config

    def _get_statements(self, tree: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get all statements from the compilation unit."""
        try:
            # Navigate to script_statements
            if tree.get("rule") == ["compilation_unit", "script_statements"]:
                return tree.get("children", [])
            return []
        except (KeyError, TypeError):
            return []

    def _is_snowflake_block(self, statement: Dict[str, Any]) -> bool:
        """Check if statement is a snowflake block: snowflake { ... }"""
        try:
            # Look for command_expression with identifier "snowflake" and a closure
            children = statement.get("children", [])
            if len(children) >= 2:
                first_child = children[0]
                if self._get_identifier_value(first_child) == "snowflake":
                    # Check if second child is a closure/block
                    second_child = children[1]
                    return self._is_closure_block(second_child)
            return False
        except (KeyError, TypeError, IndexError):
            return False

    def _is_snowflake_dot_notation(self, statement: Dict[str, Any]) -> bool:
        """Check if statement is snowflake dot notation: snowflake.property = value"""
        try:
            children = statement.get("children", [])
            if len(children) >= 3:
                first_child = children[0]
                assign_child = children[1]

                # Check for assignment
                if assign_child.get("leaf") == "ASSIGN":
                    # Check if first child is a path expression starting with "snowflake"
                    return self._is_snowflake_path_expression(first_child)
            return False
        except (KeyError, TypeError, IndexError):
            return False

    def _is_profiles_block(self, statement: Dict[str, Any]) -> bool:
        """Check if statement is a profiles block: profiles { ... }"""
        try:
            children = statement.get("children", [])
            if len(children) >= 2:
                first_child = children[0]
                if self._get_identifier_value(first_child) == "profiles":
                    return self._is_closure_block(children[1])
            return False
        except (KeyError, TypeError, IndexError):
            return False

    def _extract_from_snowflake_block(self, statement: Dict[str, Any]) -> Dict[str, Any]:
        """Extract configuration from a snowflake block."""
        config = {}
        try:
            # Get the closure block (second child)
            closure = statement.get("children", [])[1]
            block_statements = self._get_statements_from_block(closure)

            for block_statement in block_statements:
                prop_name, value = self._extract_assignment(block_statement)
                if prop_name and value is not None:
                    config[prop_name] = value

        except (KeyError, TypeError, IndexError):
            pass

        return config

    def _extract_from_dot_notation(self, statement: Dict[str, Any]) -> tuple:
        """Extract property name and value from dot notation assignment."""
        try:
            children = statement.get("children", [])
            path_expr = children[0]
            value_expr = children[2]

            # Extract property name from path expression
            prop_name = self._extract_property_from_path(path_expr)

            # Extract value
            value = self._extract_value(value_expr)

            return prop_name, value
        except (KeyError, TypeError, IndexError):
            return None, None

    def _extract_assignment(self, statement: Dict[str, Any]) -> tuple:
        """Extract property name and value from an assignment statement."""
        try:
            children = statement.get("children", [])
            if len(children) >= 3 and children[1].get("leaf") == "ASSIGN":
                prop_name = self._get_identifier_value(children[0])
                value = self._extract_value(children[2])
                return prop_name, value
        except (KeyError, TypeError, IndexError):
            pass
        return None, None

    def _extract_property_from_path(self, path_expr: Dict[str, Any]) -> Optional[str]:
        """Extract property name from a snowflake path expression."""
        try:
            children = path_expr.get("children", [])
            # Should have identifier "snowflake" and path_element with property name
            if len(children) >= 2:
                path_element = children[1]
                # Extract identifier from path_element
                path_children = path_element.get("children", [])
                for child in path_children:
                    if child.get("rule") and "identifier" in child["rule"]:
                        return self._get_identifier_value(child)
        except (KeyError, TypeError, IndexError):
            pass
        return None

    def _extract_value(self, value_expr: Dict[str, Any]) -> Any:
        """Extract value from a value expression."""
        try:
            # Look for leaf values
            if value_expr.get("leaf"):
                leaf_type = value_expr["leaf"]
                value = value_expr["value"]

                if leaf_type == "STRING_LITERAL":
                    return value  # String values come without quotes
                elif leaf_type == "BOOLEAN_LITERAL":
                    return value.lower() == "true"
                elif leaf_type == "INTEGER_LITERAL":
                    return int(value)
                elif leaf_type == "DECIMAL_LITERAL":
                    return float(value)
                else:
                    return value

            # Recursively search children for leaf values
            children = value_expr.get("children", [])
            for child in children:
                result = self._extract_value(child)
                if result is not None:
                    return result

        except (KeyError, TypeError, ValueError):
            pass
        return None

    def _get_identifier_value(self, node: Dict[str, Any]) -> Optional[str]:
        """Get identifier value from a node."""
        try:
            if node.get("leaf") == "IDENTIFIER":
                return node.get("value")

            # Search children for identifier
            children = node.get("children", [])
            for child in children:
                result = self._get_identifier_value(child)
                if result:
                    return result
        except (KeyError, TypeError):
            pass
        return None

    def _is_closure_block(self, node: Dict[str, Any]) -> bool:
        """Check if node represents a closure block."""
        try:
            rule = node.get("rule", [])
            return "closure" in rule or "block" in rule
        except (KeyError, TypeError):
            return False

    def _is_snowflake_path_expression(self, node: Dict[str, Any]) -> bool:
        """Check if node is a path expression starting with 'snowflake'."""
        try:
            children = node.get("children", [])
            if children:
                first_child = children[0]
                return self._get_identifier_value(first_child) == "snowflake"
        except (KeyError, TypeError, IndexError):
            return False

    def _find_profile_in_block(self, profiles_statement: Dict[str, Any], profile: str) -> Optional[Dict[str, Any]]:
        """Find a specific profile block within the profiles statement."""
        try:
            block_statements = self._get_statements_from_block(profiles_statement.get("children", [])[1])

            for statement in block_statements:
                children = statement.get("children", [])
                if len(children) >= 2:
                    profile_name = self._get_identifier_value(children[0])
                    if profile_name == profile:
                        return children[1]  # Return the profile's closure block
        except (KeyError, TypeError, IndexError):
            pass
        return None

    def _get_statements_from_block(self, block_node: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get statements from a block/closure node."""
        try:
            children = block_node.get("children", [])

            # Look for block_statements_opt or block_statements
            for child in children:
                rule = child.get("rule", [])
                if "block_statements_opt" in rule or "block_statements" in rule:
                    # This could be a single statement or multiple statements
                    if "block_statement" in rule:
                        # Single statement case
                        return [child]
                    else:
                        # Look deeper for block_statement(s)
                        return self._get_statements_from_block(child)

            # If no block_statements found, look for block_statement directly in children
            block_statements = []
            for child in children:
                rule = child.get("rule", [])
                if "block_statement" in rule:
                    block_statements.append(child)

            if block_statements:
                return block_statements

        except (KeyError, TypeError):
            pass
        return []

    def _extract_plugins_config(self, tree: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract plugins configuration from the parsed AST tree."""
        plugins = []

        # Get all statements from the script
        statements = self._get_statements(tree)

        for statement in statements:
            # Look for plugins block: plugins { ... }
            if self._is_plugins_config_block(statement):
                plugins_list = self._extract_from_plugins_block(statement)
                plugins.extend(plugins_list)

        return plugins

    def _is_plugins_config_block(self, statement: Dict[str, Any]) -> bool:
        """Check if statement is a plugins configuration block: plugins { ... }"""
        try:
            children = statement.get("children", [])
            if len(children) >= 2:
                first_child = children[0]
                if self._get_identifier_value(first_child) == "plugins":
                    return self._is_closure_block(children[1])
            return False
        except (KeyError, TypeError, IndexError):
            return False

    def _extract_from_plugins_block(self, statement: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract plugin information from a plugins block."""
        plugins = []
        try:
            # Get the closure block (second child)
            closure = statement.get("children", [])[1]
            block_statements = self._get_statements_from_block(closure)

            for block_statement in block_statements:
                plugin_info = self._extract_plugin_declaration(block_statement)
                if plugin_info:
                    plugins.append(plugin_info)

        except (KeyError, TypeError, IndexError):
            pass

        return plugins

    def _extract_plugin_declaration(self, statement: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Extract plugin name and version from a plugin declaration like 'id nf-snowflake@0.8.0'"""
        try:
            children = statement.get("children", [])

            # Look for command_expression with 'id' identifier followed by string argument
            if len(children) >= 2:
                first_child = children[0]
                if self._get_identifier_value(first_child) == "id":
                    # The second child should be an argument_list containing the plugin specification
                    second_child = children[1]
                    plugin_spec = self._extract_value(second_child)
                    if plugin_spec and "@" in plugin_spec:
                        plugin_name, version = plugin_spec.split("@", 1)
                        return {"name": plugin_name.strip("'\""), "version": version.strip("'\"")}
                    elif plugin_spec:
                        # Plugin without version
                        return {"name": plugin_spec.strip("'\""), "version": None}
        except (KeyError, TypeError, IndexError, ValueError):
            pass
        return None
