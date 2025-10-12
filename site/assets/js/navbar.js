/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Progressive Navigation and Mobile Sidebar
document.addEventListener('DOMContentLoaded', function() {
    // Mobile sidebar functionality (DevLake-style slide-out)
    const navbarToggler = document.querySelector('.navbar-toggler');
    const mobileOverlay = document.getElementById('mobile_overlay');
    const mobileSidebar = document.getElementById('sidebar_menu');
    const sidebarClose = document.querySelector('.mobile-sidebar-close');
    
    function openMobileSidebar() {
        if (mobileOverlay && mobileSidebar) {
            mobileOverlay.classList.add('show');
            mobileSidebar.classList.add('show');
            navbarToggler?.setAttribute('aria-expanded', 'true');
            document.body.style.overflow = 'hidden'; // Prevent scrolling
        }
    }
    
    function closeMobileSidebar() {
        if (mobileOverlay && mobileSidebar) {
            mobileOverlay.classList.remove('show');
            mobileSidebar.classList.remove('show');
            navbarToggler?.setAttribute('aria-expanded', 'false');
            document.body.style.overflow = ''; // Restore scrolling
        }
    }
    
    // Open sidebar when hamburger is clicked
    if (navbarToggler) {
        navbarToggler.addEventListener('click', function(e) {
            e.preventDefault();
            openMobileSidebar();
        });
    }
    
    // Close sidebar when close button is clicked
    if (sidebarClose) {
        sidebarClose.addEventListener('click', function(e) {
            e.preventDefault();
            closeMobileSidebar();
        });
    }
    
    // Close sidebar when overlay is clicked
    if (mobileOverlay) {
        mobileOverlay.addEventListener('click', function() {
            closeMobileSidebar();
        });
    }
    
    // Close sidebar on escape key
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && mobileSidebar?.classList.contains('show')) {
            closeMobileSidebar();
        }
    });
    
    // Mobile dropdown toggles
    const mobileDropdownToggles = document.querySelectorAll('.mobile-nav-toggle');
    mobileDropdownToggles.forEach(toggle => {
        toggle.addEventListener('click', function(e) {
            e.preventDefault();
            const dropdownMenu = toggle.parentElement.querySelector('.mobile-dropdown-menu');
            
            if (dropdownMenu) {
                const isExpanded = dropdownMenu.classList.contains('show');
                
                // Close all other dropdowns
                document.querySelectorAll('.mobile-dropdown-menu.show').forEach(menu => {
                    if (menu !== dropdownMenu) {
                        menu.classList.remove('show');
                        menu.parentElement.querySelector('.mobile-nav-toggle').classList.remove('expanded');
                    }
                });
                
                // Toggle current dropdown
                if (isExpanded) {
                    dropdownMenu.classList.remove('show');
                    toggle.classList.remove('expanded');
                } else {
                    dropdownMenu.classList.add('show');
                    toggle.classList.add('expanded');
                }
            }
        });
    });
    
    // Close mobile sidebar when clicking on a link (for better UX)
    const mobileNavLinks = document.querySelectorAll('.mobile-nav-link:not(.mobile-nav-toggle), .mobile-dropdown-item');
    mobileNavLinks.forEach(link => {
        link.addEventListener('click', function() {
            // Small delay to allow navigation to start
            setTimeout(closeMobileSidebar, 100);
        });
    });
    
    // Expandable search functionality
    const searchToggleBtn = document.querySelector('.search-toggle-btn');
    const searchInputWrapper = document.querySelector('.search-input-wrapper');
    const searchInput = searchInputWrapper?.querySelector('input[type="search"]');
    
    if (searchToggleBtn && searchInputWrapper) {
        searchToggleBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            
            // Toggle expanded state
            const isExpanded = searchInputWrapper.classList.contains('expanded');
            
            if (isExpanded) {
                // Collapse search
                searchInputWrapper.classList.remove('expanded');
                searchToggleBtn.style.display = 'inline-flex';
            } else {
                // Expand search
                searchInputWrapper.classList.add('expanded');
                searchToggleBtn.style.display = 'none';
                
                // Focus the search input after animation
                setTimeout(() => {
                    if (searchInput) {
                        searchInput.focus();
                    }
                }, 100);
            }
        });
        
        // Collapse search when clicking outside
        document.addEventListener('click', function(e) {
            const searchContainer = document.querySelector('.search-expandable');
            if (searchContainer && !searchContainer.contains(e.target)) {
                searchInputWrapper.classList.remove('expanded');
                searchToggleBtn.style.display = 'inline-flex';
            }
        });
        
        // Collapse search on escape key
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && searchInputWrapper.classList.contains('expanded')) {
                searchInputWrapper.classList.remove('expanded');
                searchToggleBtn.style.display = 'inline-flex';
            }
        });
    }
    
    // Simplified navigation - no more "More" dropdown needed
});
